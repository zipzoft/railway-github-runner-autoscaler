package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"time"
)

const (
	githubAPIURL = "https://api.github.com"
	// githubAPIVersion pins the REST schema. Without it GitHub is free to serve a
	// newer default, and the one field this service reads — job status — is the
	// field a schema change would move.
	githubAPIVersion = "2022-11-28"
	// githubRequestTimeout bounds one job lookup. It is deliberately tighter than
	// the Railway client's 10s: reconcile issues one of these per tracked id, so
	// the per-request bound multiplies, and reconcileBudget has to be able to fit
	// a useful number of them.
	githubRequestTimeout = 5 * time.Second
)

// jobEntry is one tracked, unfinished job.
//
// since is when the job entered its CURRENT set — it is reset on the
// queued→inProgress move, so a job that waited hours in the queue gets a fresh
// StaleJobTTL horizon once it starts running rather than being reaped mid-job.
// That is the pre-existing behaviour and reapStaleJobs depends on it.
//
// repo is the "owner/name" the job belongs to, taken from the webhook payload:
// GitHub's job endpoint is repo-scoped, so without it an id cannot be looked up
// at all. It is empty for any entry planted before this field existed, and
// reconcile skips those rather than guessing a repo — guessing would produce a
// 404, and a 404 is exactly the answer that must never be read as "finished".
//
// seq identifies this particular tracked-ness of the id, and exists ONLY so
// reconcile's commit can tell "the entry I asked GitHub about" from "an entry a
// webhook created for the same id while I was asking". `since` cannot serve that
// purpose: it is a wall-clock reading, so a delete-then-reinsert inside the same
// clock tick produces an identical value — trivially so under the frozen clock
// every test in this package uses, and merely improbably so in production. A
// safety property whose test can only fail by luck is not a tested property.
type jobEntry struct {
	since time.Time
	repo  string
	seq   uint64
}

// jobLiveness is what GitHub says about one job id. The three values are NOT
// interchangeable and the distinction is the whole safety argument of reconcile:
// only jobFinished is authority to forget an id. jobUnknown covers every way the
// question failed to get an answer — a network error, a 5xx, a rate limit, and
// critically a 404, which GitHub also returns for a private repo the token
// cannot see. Pruning on jobUnknown would let a token scoped to one repo delete
// live jobs belonging to every other repo and shrink the fleet under them.
type jobLiveness int

const (
	jobUnknown jobLiveness = iota
	jobActive
	jobFinished
)

func (l jobLiveness) String() string {
	switch l {
	case jobActive:
		return "active"
	case jobFinished:
		return "finished"
	default:
		return "unknown"
	}
}

// errJobNotFound is the 404 case, kept distinct from a transport failure so the
// log can tell an operator which of the two explanations applies: the run was
// deleted, or the token cannot read that repository.
var errJobNotFound = errors.New("github: job not found")

// errRateLimited means GitHub asked us to back off. Reconcile abandons the rest
// of the cycle on it rather than spending its remaining budget on requests that
// will be refused anyway — and, worse, deepening a secondary rate limit.
var errRateLimited = errors.New("github: rate limited")

// GitHubClient answers "is this job still alive?" for one job id. It is an
// interface so tests can substitute a fake without network access.
type GitHubClient interface {
	JobStatus(ctx context.Context, repo string, id int64) (jobLiveness, error)
}

// githubClient is the production GitHubClient. It reads
// GET /repos/{owner}/{repo}/actions/jobs/{job_id}, which needs only the
// `actions:read` scope — a fine-grained PAT is enough, no admin:org.
type githubClient struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

func newGitHubClient(token string) *githubClient {
	return &githubClient{
		token:   token,
		baseURL: githubAPIURL,
		// An explicit timeout, for the same reason the Railway client has one:
		// a hung backend must not pin the reconcile goroutine. Reconcile makes
		// this call once per tracked id, so the bound also caps how long a whole
		// cycle can take.
		httpClient: &http.Client{Timeout: githubRequestTimeout},
	}
}

func (c *githubClient) JobStatus(ctx context.Context, repo string, id int64) (jobLiveness, error) {
	url := fmt.Sprintf("%s/repos/%s/actions/jobs/%d", c.baseURL, repo, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return jobUnknown, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return jobUnknown, fmt.Errorf("github api: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return jobUnknown, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return jobUnknown, errJobNotFound
	}
	if resp.StatusCode != http.StatusOK {
		// 403 collapses three different situations — a primary rate limit, a
		// secondary rate limit, and a genuinely forbidden resource. The safe
		// action is the same for all three (keep the entry), but the operator
		// response is not, and hammering a rate-limited API every tick with no
		// backoff and no signal is its own defect. Surface what GitHub said.
		remaining := resp.Header.Get("x-ratelimit-remaining")
		retryAfter := resp.Header.Get("retry-after")
		if retryAfter != "" || remaining == "0" {
			return jobUnknown, fmt.Errorf("%w: status %d, x-ratelimit-remaining=%q retry-after=%q",
				errRateLimited, resp.StatusCode, remaining, retryAfter)
		}
		return jobUnknown, fmt.Errorf("github api returned %d (x-ratelimit-remaining=%q)", resp.StatusCode, remaining)
	}

	var out struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return jobUnknown, fmt.Errorf("unmarshal response: %w", err)
	}
	switch out.Status {
	case "completed":
		return jobFinished, nil
	case "":
		// A 200 with no status field is a schema surprise, not a completion.
		// Falling through to "finished" here would delete live ids on the day
		// GitHub renames the field.
		return jobUnknown, fmt.Errorf("github returned no status for job %d", id)
	default:
		// queued, in_progress, waiting, requested, pending — every non-terminal
		// status GitHub documents, plus any it adds later. Treating an unfamiliar
		// non-empty status as alive is the safe default.
		return jobActive, nil
	}
}

// noteBlindCycle tracks consecutive reconcile passes that resolved nothing but
// 404s and warns once the run is long enough to be a configuration fault rather
// than a coincidence. Without it the 404-keeps-the-entry rule — which is correct
// — also makes a mis-scoped token completely silent.
func (s *Server) noteBlindCycle(blind bool) {
	const blindCycleWarnAfter = 3

	s.state.mu.Lock()
	if !blind {
		s.state.blindCycles = 0
		s.state.mu.Unlock()
		return
	}
	s.state.blindCycles++
	n := s.state.blindCycles
	s.state.mu.Unlock()

	if n == blindCycleWarnAfter {
		log.Printf("[WARN] reconcile: %d consecutive cycles in which GitHub recognised NONE of the tracked "+
			"jobs. Reconcile is running but retiring nothing, so leaks still clear on the %s horizon only. "+
			"The usual cause is a GITHUB_TOKEN without actions:read on the repositories these jobs run in",
			n, s.cfg.StaleJobTTL)
	}
}

// reconcile asks GitHub whether each tracked job is really still unfinished and
// forgets the ones GitHub reports as completed.
//
// It exists because a counter fed only by webhooks leaks every time a webhook is
// lost. ATT-482 removed the deadlock that leak used to cause, but not the leak:
// an id whose `completed` delivery never arrived sits in the tracked set until
// reapStaleJobs purges it at the StaleJobTTL horizon — 7 hours by default — and
// for all of that time the fleet is pinned above its idle baseline. That is no
// longer an outage, but it is a bill. Reconciling makes every leak of that class
// correct itself within one tick instead.
//
// Three properties carry the safety argument:
//
//  1. ONLY an authoritative "completed" is authority to forget an id. Every
//     other outcome — a transport failure, a 5xx, a rate limit, and in
//     particular a 404 — keeps the entry, because GitHub answers 404 both for an
//     id it has never heard of and for a private repository the token cannot
//     read. A token scoped to one repo would otherwise delete every live job
//     belonging to every other repo in the org and shrink the fleet under them.
//     The leak this exists to fix is a lost terminal webhook, and in that case
//     the job really did finish, so GitHub answers 200 + completed. Anything
//     else keeps reapStaleJobs as its backstop, exactly as today.
//
//  2. The GitHub round-trips happen with NO locks held, for the same reason
//     seedFloorOnce reads Railway that way: a webhook must never queue behind a
//     slow external call, because GitHub gives a delivery about ten seconds
//     before it gives up and a delivery this process never receives is a job it
//     can never assert a replica for.
//
//  3. Because of (2) the snapshot can be stale by the time it is committed, so
//     the commit is a compare-and-delete on the entry's `since`: an id a webhook
//     re-registered during the lookup window is left alone rather than deleted
//     out from under the scale decision that just accounted for it.
//
// It deliberately does NOT re-bucket an id when GitHub disagrees about
// queued-vs-inProgress. apply() scales on the SUM of the two sets, so moving an
// id between them cannot change the replica count, while resetting its `since`
// to do so would let a genuinely stuck job evade the TTL horizon forever.
//
// It also cannot DISCOVER work — it can only ask about ids it already tracks —
// so it does not remove the boot-era replica floor. Jobs already running when
// this process started stay invisible to it, and time remains the only honest
// bound on those. See State.
func (s *Server) reconcile(ctx context.Context) {
	if s.github == nil {
		// No token configured. Reconcile is off and StaleJobTTL is the only
		// cleanup, which is precisely the behaviour that shipped before this
		// existed — so an unconfigured deployment is no worse off, just slower
		// to shed a leak.
		return
	}

	type candidate struct {
		id    int64
		entry jobEntry
		// set is the map this entry was snapshotted from, so the commit deletes
		// from that set and no other.
		set map[int64]jobEntry
	}

	s.state.mu.Lock()
	tracked := len(s.state.queued) + len(s.state.inProgress)
	cands := make([]candidate, 0, tracked)
	for id, e := range s.state.queued {
		if e.repo != "" {
			cands = append(cands, candidate{id: id, entry: e, set: s.state.queued})
		}
	}
	for id, e := range s.state.inProgress {
		if e.repo != "" {
			cands = append(cands, candidate{id: id, entry: e, set: s.state.inProgress})
		}
	}
	s.state.mu.Unlock()

	if skipped := tracked - len(cands); skipped > 0 {
		// Not cosmetic. If the webhook payload ever stops carrying
		// `repository.full_name`, EVERY entry lands here and reconcile silently
		// becomes the no-op it looks like it is not. Say so out loud instead.
		log.Printf("[WARN] reconcile: %d of %d tracked job(s) carry no repository and cannot be "+
			"checked against GitHub; they clear on the %s horizon only", skipped, tracked, s.cfg.StaleJobTTL)
	}
	if len(cands) == 0 {
		return
	}

	// Oldest first, id as a tie-break so a capped cycle is deterministic. The
	// oldest entries are the ones nearest the TTL horizon and the ones most
	// likely to be leaked, so they are where a bounded budget belongs.
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].entry.since.Equal(cands[j].entry.since) {
			return cands[i].id < cands[j].id
		}
		return cands[i].entry.since.Before(cands[j].entry.since)
	})
	if len(cands) > reconcileMaxPerCycle {
		log.Printf("reconcile: %d tracked job(s) exceed the %d-per-cycle budget; checking the oldest %d, "+
			"the rest next cycle", len(cands), reconcileMaxPerCycle, reconcileMaxPerCycle)
		cands = cands[:reconcileMaxPerCycle]
	}

	// --- lookups, no locks held ---
	var finished []candidate
	var checked, active int
	// Collected rather than logged per id. A mis-scoped token 404s on EVERY
	// tracked job, so a line each would put ~50 identical explanations in the log
	// every 5 minutes and bury the census that actually summarises the cycle.
	var notFoundIDs, failedIDs []int64
	var lastErr error
	rateLimited := false
	for _, c := range cands {
		if err := ctx.Err(); err != nil {
			// The wall-clock budget, not a shortage of ids. Whatever is left is
			// checked next cycle; the tick must go on to assertDesired.
			log.Printf("reconcile: budget spent after %d of %d lookup(s) (%v); the rest wait for the next cycle",
				checked, len(cands), err)
			break
		}
		live, err := s.github.JobStatus(ctx, c.entry.repo, c.id)
		checked++
		switch {
		case errors.Is(err, errRateLimited):
			// Stop asking, but keep what has already been answered: the
			// completions confirmed earlier in this cycle came back 200 and are
			// no less authoritative for what happened afterwards. Break rather
			// than return so they still commit and the census still prints.
			checked-- // the refused request answered nothing
			rateLimited = true
			log.Printf("[WARN] reconcile: GitHub rate-limited this cycle after %d lookup(s) (%v); "+
				"abandoning the rest rather than deepening it", checked, err)
		case errors.Is(err, errJobNotFound):
			notFoundIDs = append(notFoundIDs, c.id)
		case err != nil:
			failedIDs = append(failedIDs, c.id)
			lastErr = err
		case live == jobFinished:
			finished = append(finished, c)
		default:
			active++
		}
		if rateLimited {
			break
		}
	}

	// A census every cycle, because the alternative is a feature that cannot be
	// observed: with 404-keeps-the-entry, a token scoped to the wrong
	// repositories produces a service that looks healthy, logs nothing alarming,
	// and does exactly nothing.
	if len(notFoundIDs) > 0 {
		log.Printf("reconcile: GitHub does not recognise %d job(s): %v. Either their runs were deleted or "+
			"GITHUB_TOKEN cannot read the repositories they ran in. Keeping every one — a 404 is not a "+
			"completion, and GitHub answers 404 for a repository a token cannot see, so pruning on one "+
			"would delete live jobs. They clear on the %s horizon", len(notFoundIDs), notFoundIDs, s.cfg.StaleJobTTL)
	}
	if len(failedIDs) > 0 {
		log.Printf("reconcile: could not check %d job(s): %v (last error: %v); keeping every one",
			len(failedIDs), failedIDs, lastErr)
	}
	log.Printf("reconcile: checked=%d finished=%d active=%d notfound=%d error=%d (of %d tracked)",
		checked, len(finished), active, len(notFoundIDs), len(failedIDs), tracked)
	// A cycle cut short by a rate limit says nothing about whether the token can
	// see these repositories, so it must not count toward the blind run — in
	// either direction.
	if !rateLimited {
		s.noteBlindCycle(checked > 0 && len(notFoundIDs) == checked)
	}

	if len(finished) == 0 {
		return
	}

	// --- commit ---
	// scaleMu so the prune is ordered strictly before or after a scale decision,
	// never between one's read of the counters and its push.
	s.scaleMu.Lock()
	defer s.scaleMu.Unlock()

	s.state.mu.Lock()
	var pruned []int64
	for _, c := range finished {
		// Compare-and-delete on the entry's identity, not its timestamp, and
		// only in the set it was snapshotted from. A webhook may have retired
		// this id and re-registered it, or moved it between the two sets, while
		// the lookup was in flight; that newer entry is a different tracked job
		// as far as this process is concerned, and GitHub did not answer for it.
		if e, ok := c.set[c.id]; ok && e.seq == c.entry.seq {
			delete(c.set, c.id)
			pruned = append(pruned, c.id)
		}
	}
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	s.state.mu.Unlock()

	if len(pruned) == 0 {
		return
	}
	// The replica count is deliberately NOT pushed here. tick() runs
	// assertDesired immediately after, which is the one place that decides and
	// pushes a count — keeping the new code out of the path that can cancel a
	// running job. The contraction therefore still lands in this same cycle.
	log.Printf("reconcile: retired %d job(s) GitHub reports complete, %s ahead of the TTL horizon: %v "+
		"(queued=%d inProgress=%d)", len(pruned), s.cfg.StaleJobTTL, pruned, queued, inProgress)
}

// probe checks at boot that GITHUB_TOKEN authenticates at all. /rate_limit needs
// no scope, so a failure here is unambiguously a bad or expired credential —
// distinct from the scope problem noteBlindCycle catches later. Without it, an
// operator who has just set the token has no confirmation until the next leak
// happens to appear, hours away.
func (c *githubClient) probe(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/rate_limit", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("github /rate_limit returned %d", resp.StatusCode)
	}
	return nil
}

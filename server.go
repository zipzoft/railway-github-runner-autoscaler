package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	railwayGQLURL = "https://backboard.railway.app/graphql/v2"
	maxBodyBytes  = 5 * 1024 * 1024 // 5MB
)

type WorkflowJobEvent struct {
	Action      string      `json:"action"`
	WorkflowJob WorkflowJob `json:"workflow_job"`
	// Repository is the repo the job belongs to. Every repository-scoped GitHub
	// webhook carries it, and reconcile cannot ask GitHub about a job id without
	// it: the job endpoint is repo-scoped, and a job id queried against the wrong
	// repo returns 404 (verified against the live API).
	Repository Repository `json:"repository"`
}

type WorkflowJob struct {
	ID     int64    `json:"id"`
	Labels []string `json:"labels"`
}

type Repository struct {
	FullName string `json:"full_name"`
}

type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

type gqlResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}

	if !validateHMAC(body, r.Header.Get("X-Hub-Signature-256"), s.cfg.WebhookSecret) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	if r.Header.Get("X-GitHub-Event") != "workflow_job" {
		w.WriteHeader(http.StatusOK)
		return
	}

	var event WorkflowJobEvent
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if !hasLabels(event.WorkflowJob.Labels, s.cfg.RunnerLabels) {
		log.Printf("webhook ignored: labels %v do not match required %v", event.WorkflowJob.Labels, s.cfg.RunnerLabels)
		w.WriteHeader(http.StatusOK)
		return
	}

	log.Printf("webhook received: action=%s labels=%v", event.Action, event.WorkflowJob.Labels)

	id := event.WorkflowJob.ID
	// repo is the only webhook field this service interpolates into an outbound
	// URL (github.go's job endpoint) and into log lines. It is HMAC-gated, so
	// this is hardening rather than a hole, but it is the one value crossing a
	// trust boundary and it is never otherwise checked. A non-conforming value
	// degrades to "" — a state every path already handles safely: reconcile
	// skips the entry and adoption refuses it.
	repo := event.Repository.FullName
	if repo != "" && !validRepo(repo) {
		log.Printf("[WARN] webhook: repository %q is not a plausible owner/name; job %d will be tracked "+
			"without one and can only clear on the %s horizon", repo, id, s.cfg.StaleJobTTL)
		repo = ""
	}
	// Scaling side-effects run on a background context with their own deadline,
	// so a GitHub delivery connection that drops after the job state was already
	// recorded can't cancel the scale mid-flight. Non-scaling actions ignore it.
	scaleCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	switch event.Action {
	case "queued":
		if err := s.scaleUp(scaleCtx, id, repo); err != nil {
			log.Printf("scale up error: %v", err)
			http.Error(w, "failed to scale up", http.StatusInternalServerError)
			return
		}
	case "in_progress":
		s.markInProgress(id, repo)
	case "completed":
		// completed is GitHub's only terminal workflow_job action: it fires whether
		// the job ran to completion, failed, or was cancelled before ever starting
		// (e.g. superseded by concurrency.cancel-in-progress). scaleDown must retire
		// the id from every in-flight set here, since no other event will.
		if err := s.scaleDown(scaleCtx, id); err != nil {
			log.Printf("scale down error: %v", err)
			http.Error(w, "failed to scale down", http.StatusInternalServerError)
			return
		}
	default:
		log.Printf("webhook ignored: action=%s not handled", event.Action)
	}

	w.WriteHeader(http.StatusOK)
}

func validateHMAC(body []byte, sigHeader, secret string) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(sigHeader, prefix) {
		return false
	}
	provided, err := hex.DecodeString(sigHeader[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(mac.Sum(nil), provided)
}

// repoPattern is GitHub's shape for "owner/name". The owner half is the
// stricter of the two — GitHub logins are alphanumerics and single hyphens, with
// no dots or underscores — and the repository half allows dot, underscore and
// hyphen.
//
// The pattern alone is NOT sufficient, which is the whole reason validRepo is a
// function rather than one call: `.` and `..` are made entirely of characters
// the repository half allows, so `a/..` matches happily and yields the wire path
// /repos/a/../actions/jobs/N. That value goes out with an Authorization header
// on it, so dot segments are rejected explicitly below.
var repoPattern = regexp.MustCompile(`^[A-Za-z0-9](?:-?[A-Za-z0-9]){0,38}/[A-Za-z0-9._-]{1,100}$`)

func validRepo(repo string) bool {
	if !repoPattern.MatchString(repo) {
		return false
	}
	// A path segment of "." or ".." is a traversal, not a name. GitHub rejects
	// both as repository names anyway, so nothing legitimate is lost.
	owner, name, _ := strings.Cut(repo, "/")
	return owner != "." && owner != ".." && name != "." && name != ".."
}

// hasLabels returns true if every label in required appears in jobLabels (case-insensitive).
func hasLabels(jobLabels, required []string) bool {
	lower := make(map[string]struct{}, len(jobLabels))
	for _, l := range jobLabels {
		lower[strings.ToLower(l)] = struct{}{}
	}
	for _, req := range required {
		if _, ok := lower[req]; !ok {
			return false
		}
	}
	return true
}

// markInProgress moves a job from queued to inProgress, and — when reconcile is
// available to bound the cost — adopts one it never saw queued.
//
// The refusal to adopt was correct while a phantom could only be cleared by the
// StaleJobTTL reaper: an in_progress redelivered after the job had already
// completed would have pinned a replica for 7 hours. But refusing has its own
// cost, and it is the worse one. A job whose `queued` delivery was lost runs to
// completion in NO set at all: live, occupying a replica, invisible. Nothing
// counts it, so nothing holds a floor for it, and the next decision that finds
// the counters empty scales the fleet down on top of it.
//
// Until now that job was protected only by accident — by the very leak this card
// removes, which happened to hold the fleet up for longer than GitHub's own
// default job timeout. Clearing leaks within one tick takes that accident away,
// so the untracked job has to be made visible deliberately instead.
//
// Reconcile is what makes adoption safe: an adopted phantom is checked against
// GitHub within one tick and retired if it really had finished. That trades an
// UNRECOVERABLE error (cancelling a running job) for a RECOVERABLE one (one
// spare replica for at most one cycle).
//
// The bound only holds where reconcile can actually answer, so adoption waits
// for proof rather than for configuration. A GITHUB_TOKEN merely being SET
// proves nothing: an expired one fails every lookup, and one without
// actions:read on this job's repository 404s on it forever — and a 404 must
// never retire an entry, so the phantom would hold a replica for the full TTL.
// That is the harm refusing to adopt existed to prevent. So the gate is
// State.healthyRepos: this process must have seen GitHub answer a lookup for
// THIS repository before it will adopt a job in it. Until then, and whenever no
// client is configured at all, the old refusal stands unchanged.
func (s *Server) markInProgress(id int64, repo string) {
	s.state.mu.Lock()
	if _, queued := s.state.queued[id]; !queued {
		if s.github == nil || repo == "" || !s.state.healthyRepos[repo] {
			// Nothing here can bound a phantom's lifetime, so keep refusing: a
			// late or retried in_progress for an already-completed job would
			// otherwise hold the fleet above its idle baseline until the TTL
			// reaper.
			why := "late or out-of-order webhook"
			if s.github != nil && repo != "" {
				why = "GitHub has not yet answered a lookup for " + repo +
					", so an adopted phantom could not be retired"
			}
			s.state.mu.Unlock()
			log.Printf("in_progress ignored: job %d is not queued (%s)", id, why)
			return
		}
		s.state.inProgress[id] = jobEntry{since: s.clock(), repo: repo, seq: s.state.nextSeq()}
		inProgress := len(s.state.inProgress)
		s.state.mu.Unlock()
		log.Printf("job adopted: id=%d in %s reported in_progress without a queued this process saw; "+
			"tracking it so no scale decision shrinks the fleet under it (inProgress=%d). Reconcile "+
			"retires it within one cycle if GitHub says it has already finished", id, repo, inProgress)
		return
	}
	// Carry the entry across rather than rebuilding it, so `repo` survives the
	// move — reconcile cannot ask GitHub about an id it has no repository for.
	// `since` is deliberately reset: the TTL horizon measures how long a job has
	// been in its CURRENT state, so a job that queued for hours gets a full
	// horizon once it actually starts running. `seq` is renewed because this is a
	// new insertion, and a reconcile lookup issued against the queued entry must
	// not commit against the inProgress one.
	entry := s.state.queued[id]
	delete(s.state.queued, id)
	entry.since = s.clock()
	entry.seq = s.state.nextSeq()
	s.state.inProgress[id] = entry
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	s.state.mu.Unlock()
	log.Printf("job in progress: id=%d queued=%d inProgress=%d", id, queued, inProgress)
}

func (s *Server) scaleUp(ctx context.Context, id int64, repo string) error {
	s.scaleMu.Lock()
	defer s.scaleMu.Unlock()

	s.state.mu.Lock()
	s.state.queued[id] = jobEntry{since: s.clock(), repo: repo, seq: s.state.nextSeq()}
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	s.state.mu.Unlock()

	next, err := s.apply(ctx)
	if err != nil {
		return err
	}
	if queued+inProgress > s.cfg.MaxRunners {
		log.Printf("at max runners (%d): job %d waiting behind the backlog, replicas held at %d (queued=%d inProgress=%d)",
			s.cfg.MaxRunners, id, next, queued, inProgress)
		return nil
	}
	log.Printf("scaled up: replicas=%d (job id=%d, queued=%d inProgress=%d)", next, id, queued, inProgress)
	return nil
}

func (s *Server) scaleDown(ctx context.Context, id int64) error {
	s.scaleMu.Lock()
	defer s.scaleMu.Unlock()

	s.state.mu.Lock()
	// A job that is cancelled while still queued (e.g. superseded by
	// concurrency.cancel-in-progress) never fires in_progress, so it must be
	// retired from queued here too - otherwise its id is never removed from any
	// set and the queued count leaks upward forever.
	delete(s.state.queued, id)
	delete(s.state.inProgress, id)
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	s.state.mu.Unlock()

	if inProgress > 0 {
		// Decreasing the replicas while jobs are still in progress can cause them
		// to be killed before completion, so hold the count until the batch drains.
		log.Printf("scaled down: job %d complete, queued=%d inProgress=%d, replicas unchanged", id, queued, inProgress)
		return nil
	}

	next, err := s.apply(ctx)
	if err != nil {
		return err
	}
	if queued == 0 {
		log.Printf("scaled down: all jobs complete, replicas=%d", next)
	} else {
		log.Printf("scaled down: in-progress batch done, resuming %d pending job(s) with %d replica(s)", queued, next)
	}
	return nil
}

// apply pushes the replica count the current work set needs and returns the
// value applied. One replica per unfinished job, clamped to [1, MaxRunners]:
// finished jobs are not counted, because a completed job needs no runner.
//
// Three properties matter, and the first two were missing before ATT-482:
//
//  1. It is called on EVERY scale decision, including when the backlog is over
//     the cap. Bailing out with "at max runners" instead meant no runner could
//     start, so no job could complete, so no terminal webhook could arrive to
//     unwind the count: a closed loop that held CI down for 2.5h.
//
//  2. While any job is outstanding the count never goes DOWN. Railway is free to
//     drop any replica when numReplicas shrinks, including one mid-job, so
//     neither a newly-queued job nor a restart of this process may shrink a fleet
//     that is still working. state.applied is that floor, seeded from Railway's
//     live count at boot (see seedFloor, with the cap as the fallback). A second
//     floor covers the jobs that were already running at boot and can never be
//     tracked; it expires on the StaleJobTTL horizon rather than on any observed
//     drain, because no sequence of webhooks can prove an invisible job finished.
//
//  3. An unchanged count is re-pushed, but not more often than coalesceWindow.
//     Re-asserting is what revives a fleet whose replicas died unobserved, so it
//     cannot be skipped outright; issuing it on all 30 webhooks of a burst,
//     serialized behind scaleMu, would push the last delivery past GitHub's
//     timeout. assertDesired covers the suppressed window.
//
// It reads the counters ITSELF, under state.mu, rather than taking them from the
// caller. Callers used to snapshot, release state.mu, and pass the numbers down,
// and that was safe only while every path that could GROW the work set also held
// scaleMu. Adoption broke that assumption: markInProgress takes state.mu alone
// (it must — an in_progress webhook cannot wait behind a Railway round-trip), so
// a job could be adopted after a caller's snapshot and before this computed,
// leaving the "never shrink while work is outstanding" floor to be tested
// against stale zeros and a live job's replica taken away.
//
// A window remains between this read and the push below, which is a network
// call and cannot be inside the lock. assertDesired re-asserts every tick, so
// the fleet recovers; a job squeezed inside that window does not. Narrowing it
// to the push is the most that can be done without putting webhooks behind
// Railway.
//
// Callers must hold scaleMu.
func (s *Server) apply(ctx context.Context) (int, error) {
	s.state.mu.Lock()
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	next := max(1, min(queued+inProgress, s.cfg.MaxRunners))
	// Two floors, for the two kinds of work that can be running.
	//
	// Tracked work: while this process has jobs outstanding, never go below what
	// it last pushed.
	clampedBy := ""
	if queued+inProgress > 0 && next < s.state.applied {
		next = s.state.applied
		clampedBy = "tracked work still outstanding"
	}
	// Boot-era work: jobs already running when this process started are
	// permanently invisible to it, so its counters reading zero proves nothing
	// until no such job could still be alive. See State.
	if s.clock().Before(s.state.bootFloorUntil) && next < s.state.bootFloor {
		next = s.state.bootFloor
		clampedBy = fmt.Sprintf("boot-era floor, held until %s",
			s.state.bootFloorUntil.Format(time.RFC3339))
	}
	unchanged := next == s.state.applied
	recent := !s.state.lastPush.IsZero() && s.clock().Sub(s.state.lastPush) < coalesceWindow
	s.state.mu.Unlock()

	if unchanged && recent {
		return next, nil
	}

	if err := s.client.SetReplicas(ctx, next); err != nil {
		// Leave applied untouched. If the push failed the fleet may still be
		// larger than we asked for, and a stale-high floor over-provisions
		// rather than shrinking under a running job.
		return next, err
	}

	s.state.mu.Lock()
	s.state.applied = next
	s.state.lastPush = s.clock()
	s.state.mu.Unlock()

	if clampedBy != "" {
		// The healthy signal that a floor is doing its job. Without it the only
		// visible evidence is a replica count that silently refuses to fall, and
		// an operator watching a deploy cannot tell that from a stuck service.
		log.Printf("replicas held at %d rather than %d: %s",
			next, max(1, min(queued+inProgress, s.cfg.MaxRunners)), clampedBy)
	}
	return next, nil
}

// nextSeq returns a fresh entry identity. Callers must hold state.mu.
func (st *State) nextSeq() uint64 {
	st.seq++
	return st.seq
}

// reapLoop runs the background tick until ctx is cancelled.
func (s *Server) reapLoop(ctx context.Context) {
	every := s.tickInterval
	if every == 0 {
		every = reapInterval
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

// tick is one pass of the background loop: purge entries whose terminal webhook
// never arrived, then re-assert the replica count the remaining work implies.
// Both halves are needed — the reap alone cannot revive a fleet, and the assert
// alone cannot clear a phantom.
func (s *Server) tick(ctx context.Context) {
	// Reconcile gets its own deadline so a slow or hanging GitHub cannot eat the
	// tick that reapStaleJobs and assertDesired need. See reconcileBudget: those
	// two must keep their cadence even — especially — when GitHub is degraded.
	budget := s.reconcileBudget
	if budget == 0 {
		budget = reconcileBudget
	}
	rctx, cancel := context.WithTimeout(ctx, budget)
	s.reconcile(rctx)
	cancel()

	s.reapStaleJobs(ctx)
	s.assertDesired(ctx)
}

// reapStaleJobs is a defense-in-depth safety net, not the primary leak fix
// (that is the delete-from-queued-in-scaleDown change above). It protects
// against a webhook delivery that is lost entirely - e.g. GitHub retries
// exhausted while the service was down - which would otherwise leak an id
// forever with no terminal event to clean it up. Any queued/inProgress entry
// older than cfg.StaleJobTTL is treated as abandoned and purged. The default is
// 420 minutes, an hour clear of GitHub's own default job timeout of 360 — see
// defaultStaleJobTTLMin for why that margin is not optional.
func (s *Server) reapStaleJobs(ctx context.Context) {
	s.scaleMu.Lock()
	defer s.scaleMu.Unlock()

	now := s.clock()
	s.state.mu.Lock()
	var reaped []int64
	for id, e := range s.state.queued {
		if now.Sub(e.since) > s.cfg.StaleJobTTL {
			delete(s.state.queued, id)
			reaped = append(reaped, id)
		}
	}
	for id, e := range s.state.inProgress {
		if now.Sub(e.since) > s.cfg.StaleJobTTL {
			delete(s.state.inProgress, id)
			reaped = append(reaped, id)
		}
	}
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	s.state.mu.Unlock()

	if len(reaped) == 0 {
		return
	}
	log.Printf("reaped %d stale job id(s), no terminal webhook received within %s: %v", len(reaped), s.cfg.StaleJobTTL, reaped)

	if inProgress > 0 {
		// still real work tracked as in-flight; leave replicas alone
		return
	}

	next, err := s.apply(ctx)
	if err != nil {
		log.Printf("reap: scale error: %v", err)
		return
	}
	log.Printf("reap: replicas set to %d after stale-job cleanup (queued=%d)", next, queued)
}

// assertDesired re-pushes the replica count the tracked work implies, in either
// direction, so neither correction depends on a webhook arriving.
//
// Up: webhooks fire on job state CHANGES, and a queued job that no runner ever
// picks up has no further state to change. Without this, a fleet that died after
// the last job was queued sat until StaleJobTTL. This is what makes the
// no-deadlock guarantee unconditional instead of dependent on CI staying busy.
//
// Down: a scale-down push that fails leaves applied stale-high on purpose (see
// apply), and nothing else would ever retry it — the batch is over, so no
// further webhook will call apply. Left alone that bills a full-width fleet
// until the next batch happens to drain successfully.
//
// The down direction is DEFERRED, not abandoned, while the boot-era floor is
// what pins the count: re-pushing a value that floor is already holding
// corrects nothing and would churn Railway once per tick for the whole horizon.
// It resumes the moment the horizon lapses.
func (s *Server) assertDesired(ctx context.Context) {
	s.scaleMu.Lock()
	defer s.scaleMu.Unlock()

	s.state.mu.Lock()
	queued := len(s.state.queued)
	inProgress := len(s.state.inProgress)
	// The count is only worth correcting downward if something other than the
	// boot-era floor is holding it up. While that floor is what pins the fleet,
	// re-pushing the value it pins achieves nothing and would issue one Railway
	// mutation per tick for the whole horizon — needless churn, and needless
	// exercise of the assumption that an unchanged update is inert.
	bootHeld := s.clock().Before(s.state.bootFloorUntil) && s.state.bootFloor >= s.state.applied
	contractable := s.state.applied > 1 && !bootHeld
	s.state.mu.Unlock()

	if queued+inProgress == 0 && !contractable {
		return
	}

	next, err := s.apply(ctx)
	if err != nil {
		log.Printf("periodic assert: scale error: %v", err)
		return
	}
	log.Printf("periodic assert: replicas=%d (queued=%d inProgress=%d)", next, queued, inProgress)
}

// seedFloorOnce refines the replica floor from Railway's live count, but only
// while nothing has been pushed yet. It runs concurrently with the HTTP server
// so a slow or hung Railway backend cannot delay the listener binding and fail
// the deploy healthcheck, and it holds scaleMu so it cannot interleave with a
// scale decision: a webhook arriving first simply waits, and one that already
// pushed keeps its value rather than having the pre-boot count overwrite it.
func (s *Server) seedFloorOnce(ctx context.Context) {
	// The Railway round-trip happens with NO locks held. Holding scaleMu across
	// it would block every webhook for the length of the read, and GitHub gives
	// a delivery 10 seconds before it gives up — the same lost-delivery problem
	// binding late was meant to avoid.
	floor := seedFloor(ctx, s.client, s.cfg.MaxRunners)

	// apply holds scaleMu for its whole read-compute-write, so taking it here
	// means this commit lands strictly before or strictly after a scale
	// decision, never between one's read and its write.
	s.scaleMu.Lock()
	defer s.scaleMu.Unlock()

	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if !s.state.lastPush.IsZero() {
		// A webhook got here first and pushed a value derived from real work.
		// The pre-boot count is stale next to that; adopting it would let a
		// later decision shrink under the job it just scaled up for.
		log.Printf("floor seed discarded: a scale decision already set the floor to %d", s.state.applied)
		return
	}
	s.state.applied = floor
	if floor < s.state.bootFloor {
		// Refine the conservative cap-width boot floor down to what the fleet
		// actually is. Never upward: the cap is already the safe over-estimate.
		s.state.bootFloor = floor
	}
	// Deliberately does not say "from Railway": floor is the cap fallback when
	// the read failed, and seedFloor has already logged a WARN in that case.
	log.Printf("replica floor seeded at %d (boot floor holds until %s)",
		floor, s.state.bootFloorUntil.Format(time.RFC3339))
}

// railwayClient is the production RailwayClient: it calls Railway's GraphQL
// API using a project-scoped access token.
type railwayClient struct {
	token         string
	serviceID     string
	environmentID string
	baseURL       string
	httpClient    *http.Client
}

func newRailwayClient(cfg Config) *railwayClient {
	return &railwayClient{
		token:         cfg.RailwayToken,
		serviceID:     cfg.ServiceID,
		environmentID: cfg.EnvironmentID,
		baseURL:       railwayGQLURL,
		// An explicit timeout so a hung Railway backend can't block a webhook
		// goroutine indefinitely; http.DefaultClient has none.
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *railwayClient) SetReplicas(ctx context.Context, n int) error {
	const mutation = `
mutation UpdateReplicas($serviceId: String!, $environmentId: String!, $input: ServiceInstanceUpdateInput!) {
  serviceInstanceUpdate(serviceId: $serviceId, environmentId: $environmentId, input: $input)
}`
	return c.gqlDo(ctx, gqlRequest{
		Query: mutation,
		Variables: map[string]any{
			"serviceId":     c.serviceID,
			"environmentId": c.environmentID,
			"input":         map[string]any{"numReplicas": n},
		},
	}, nil)
}

// Replicas reads the replica count Railway currently has configured for the
// runner service. Used once at boot to seed the floor; the same project-scoped
// token that authorises SetReplicas can read this, so it needs no extra
// credential.
func (c *railwayClient) Replicas(ctx context.Context) (int, error) {
	const query = `
query Replicas($serviceId: String!, $environmentId: String!) {
  serviceInstance(serviceId: $serviceId, environmentId: $environmentId) {
    numReplicas
  }
}`
	var out struct {
		ServiceInstance struct {
			NumReplicas *int `json:"numReplicas"`
		} `json:"serviceInstance"`
	}
	err := c.gqlDo(ctx, gqlRequest{
		Query: query,
		Variables: map[string]any{
			"serviceId":     c.serviceID,
			"environmentId": c.environmentID,
		},
	}, &out)
	if err != nil {
		return 0, err
	}
	if out.ServiceInstance.NumReplicas == nil {
		// Railway reports null for a service with no replica override. Treat it
		// as unknown rather than as zero, which would read as "idle fleet".
		return 0, fmt.Errorf("railway reported no numReplicas for service %s", c.serviceID)
	}
	return *out.ServiceInstance.NumReplicas, nil
}

func (c *railwayClient) gqlDo(ctx context.Context, req gqlRequest, out any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	// Project-scoped Railway tokens authenticate via Project-Access-Token, not
	// Authorization: Bearer (that header is for account/workspace/OAuth tokens).
	// This service is deployed with a project token - keep this header as-is.
	httpReq.Header.Set("Project-Access-Token", c.token)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("railway api: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("[ERROR] railway api %d | request: %s | response: %s", resp.StatusCode, body, respBody)
		return fmt.Errorf("railway api returned %d", resp.StatusCode)
	}

	var gqlResp gqlResponse
	if err := json.Unmarshal(respBody, &gqlResp); err != nil {
		log.Printf("[ERROR] railway api unmarshal | request: %s | response: %s", body, respBody)
		return fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		log.Printf("[ERROR] railway graphql | request: %s | response: %s", body, respBody)
		return fmt.Errorf("railway graphql error: %s", gqlResp.Errors[0].Message)
	}

	if out != nil && gqlResp.Data != nil {
		if err := json.Unmarshal(gqlResp.Data, out); err != nil {
			return fmt.Errorf("unmarshal data: %w", err)
		}
	}
	return nil
}

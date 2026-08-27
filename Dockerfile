FROM golang:1.22-alpine AS builder
WORKDIR /build
# Copy every Go source rather than naming them one by one. Naming them meant a
# new file (github.go) was silently left out of the image, and the build only
# failed at deploy time — CI never noticed, because the image job was skipped on
# pull_request. go build ignores _test.go files, so the glob costs nothing.
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o server .

FROM scratch
COPY --from=builder /build/server /server
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 8080
ENTRYPOINT ["/server"]

// Package server — asynchronous deploy job tracking with SSE progress streaming.
//
// The synchronous deploy pipeline (handleDeploySource) returns a single
// JSON body after the entire build → fix → push → deploy → health-check
// sequence completes. That's fine for short jobs but opaque when Claude
// is iterating on build errors (can take minutes). A long-lived async
// job ID lets the dashboard subscribe to step-by-step progress.
//
// Lifecycle:
//   1. registerDeployJob() → returns a DeployJob with an ID
//   2. caller runs the pipeline in a goroutine, calling job.Publish(evt)
//      at each phase boundary
//   3. when pipeline ends, caller invokes job.Finalize(success, result)
//   4. GET /v1/services/deploy/{id}/events streams SSE to subscribers,
//      replaying buffered history on connect so late subscribers see the
//      full timeline, then tailing live events until Done
//   5. janitor removes jobs 1 hour after Finalize (so dashboards that
//      reload can still fetch the final result for a while)
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

const (
	deployJobTTL      = 1 * time.Hour
	deployJobBufSize  = 512    // per-subscriber event buffer
	deployJobMaxHist  = 10_000 // max events retained in replay buffer
)

// DeployEvent is a single progress update emitted during an async deploy.
// Type determines which other fields are meaningful.
type DeployEvent struct {
	Type    string         `json:"type"`              // phase | log | error | done
	Phase   string         `json:"phase,omitempty"`   // extract | detect | build | aifix | push | deploy | health | done
	Message string         `json:"message,omitempty"` // human-readable
	Line    string         `json:"line,omitempty"`    // raw log line (for type=log)
	Result  map[string]any `json:"result,omitempty"`  // final pipeline result (for type=done)
	TS      string         `json:"ts"`
}

// DeployJob represents one in-flight or recently completed deploy pipeline.
type DeployJob struct {
	ID          string
	CreatedAt   time.Time
	ServiceName string
	Owner       string // requesting user; empty for token/inter-node callers

	mu          sync.Mutex
	done        bool
	finalResult map[string]any
	history     []DeployEvent
	subs        map[int]chan DeployEvent
	nextSubID   int
}

// Publisher is the callback threaded through the build pipeline so each
// step can report progress without knowing about the job registry.
type Publisher func(evt DeployEvent)

var (
	deployJobs   = map[string]*DeployJob{}
	deployJobsMu sync.Mutex
)

// registerDeployJob creates and registers a new job.
func registerDeployJob(serviceName, owner string) *DeployJob {
	id := genDeployJobID()
	j := &DeployJob{
		ID:          id,
		CreatedAt:   time.Now(),
		ServiceName: serviceName,
		Owner:       owner,
		subs:        map[int]chan DeployEvent{},
	}
	deployJobsMu.Lock()
	deployJobs[id] = j
	deployJobsMu.Unlock()
	return j
}

func getDeployJob(id string) *DeployJob {
	deployJobsMu.Lock()
	defer deployJobsMu.Unlock()
	return deployJobs[id]
}

func genDeployJobID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Publisher returns a function bound to this job that records events in
// the replay buffer and fans them out to subscribers.
func (j *DeployJob) Publisher() Publisher {
	return func(evt DeployEvent) {
		if evt.TS == "" {
			evt.TS = time.Now().UTC().Format(time.RFC3339Nano)
		}
		j.mu.Lock()
		if len(j.history) < deployJobMaxHist {
			j.history = append(j.history, evt)
		}
		subs := make([]chan DeployEvent, 0, len(j.subs))
		for _, ch := range j.subs {
			subs = append(subs, ch)
		}
		j.mu.Unlock()

		for _, ch := range subs {
			select {
			case ch <- evt:
			default:
				// Subscriber slow — drop event to avoid blocking pipeline.
			}
		}
	}
}

// Finalize marks the job done and broadcasts a final "done" event.
func (j *DeployJob) Finalize(success bool, result map[string]any) {
	evt := DeployEvent{
		Type:    "done",
		Phase:   "done",
		Message: "완료",
		Result:  result,
		TS:      time.Now().UTC().Format(time.RFC3339Nano),
	}
	if result == nil {
		evt.Result = map[string]any{"success": success}
	} else {
		if _, ok := result["success"]; !ok {
			result["success"] = success
		}
	}
	j.Publisher()(evt)

	j.mu.Lock()
	j.done = true
	j.finalResult = evt.Result
	// Close all subscriber channels so listeners exit cleanly.
	for id, ch := range j.subs {
		close(ch)
		delete(j.subs, id)
	}
	j.mu.Unlock()
}

// subscribe returns a snapshot of history plus a channel that receives
// future events until the job completes. Unsubscribe is called to drop
// the channel when the client disconnects.
func (j *DeployJob) subscribe() (history []DeployEvent, ch chan DeployEvent, unsub func(), alreadyDone bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	historyCopy := make([]DeployEvent, len(j.history))
	copy(historyCopy, j.history)

	if j.done {
		return historyCopy, nil, func() {}, true
	}
	ch = make(chan DeployEvent, deployJobBufSize)
	id := j.nextSubID
	j.nextSubID++
	j.subs[id] = ch
	unsub = func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		if _, ok := j.subs[id]; ok {
			close(ch)
			delete(j.subs, id)
		}
	}
	return historyCopy, ch, unsub, false
}

// handleDeployJobEvents streams SSE events for a given deploy job.
// Path: GET /v1/services/deploy/{id}/events
func handleDeployJobEvents(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job := getDeployJob(id)
	if job == nil {
		http.Error(w, "deploy job not found", http.StatusNotFound)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx/traefik buffering

	history, ch, unsub, alreadyDone := job.subscribe()
	defer unsub()

	// Replay buffered history.
	for _, evt := range history {
		writeSSEEvent(w, evt)
	}
	flusher.Flush()

	if alreadyDone {
		return
	}

	ctx := r.Context()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			fmt.Fprintf(w, ": hb\n\n")
			flusher.Flush()
		case evt, ok := <-ch:
			if !ok {
				return
			}
			writeSSEEvent(w, evt)
			flusher.Flush()
			if evt.Type == "done" {
				return
			}
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, evt DeployEvent) {
	data, err := json.Marshal(evt)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", data)
}

// handleDeployJobStatus returns the current known state of a deploy job
// as a single JSON blob — useful for clients that don't use SSE.
// Path: GET /v1/services/deploy/{id}
func handleDeployJobStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job := getDeployJob(id)
	if job == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"success": false, "message": "deploy job not found",
		})
		return
	}
	job.mu.Lock()
	done := job.done
	result := job.finalResult
	created := job.CreatedAt
	svc := job.ServiceName
	histLen := len(job.history)
	var lastPhase, lastMsg string
	for i := len(job.history) - 1; i >= 0; i-- {
		if job.history[i].Type == "phase" || job.history[i].Type == "error" {
			lastPhase = job.history[i].Phase
			lastMsg = job.history[i].Message
			break
		}
	}
	job.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"id":           id,
		"service":      svc,
		"created_at":   created.UTC().Format(time.RFC3339),
		"done":         done,
		"result":       result,
		"events_count": histLen,
		"last_phase":   lastPhase,
		"last_message": lastMsg,
	})
}

// deployJobJanitor runs in the background and drops jobs that finished
// more than deployJobTTL ago.
func deployJobJanitor(ctx context.Context) {
	tick := time.NewTicker(5 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			deployJobsMu.Lock()
			for id, j := range deployJobs {
				j.mu.Lock()
				expired := j.done && now.Sub(j.CreatedAt) > deployJobTTL
				j.mu.Unlock()
				if expired {
					delete(deployJobs, id)
				}
			}
			deployJobsMu.Unlock()
		}
	}
}

// noopPublisher returns a Publisher that discards events — used when a
// synchronous caller wants the pipeline to run without any progress
// streaming overhead.
func noopPublisher() Publisher {
	return func(DeployEvent) {}
}

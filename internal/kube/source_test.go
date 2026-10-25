package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s-ops-agent/internal/config"
	"k8s-ops-agent/internal/health"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func node() map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "node", "uid": "node-id", "resourceVersion": "1"},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}
}
func pod(name, reason string) map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": name, "namespace": "apps", "uid": name + "-id", "resourceVersion": "2", "creationTimestamp": time.Now().UTC().Format(time.RFC3339)},
		"spec":   map[string]any{"containers": []any{map[string]any{"name": "app", "image": "registry/app:configured", "env": []any{map[string]any{"name": "SECRET", "value": "must-not-retain"}}}}},
		"status": map[string]any{"phase": "Pending", "containerStatuses": []any{map[string]any{"name": "app", "image": "registry/app:stale", "state": map[string]any{"waiting": map[string]any{"reason": reason, "message": "must-not-retain"}}}}}}
}
func testConfig() config.Config {
	return config.Config{Namespaces: []string{"apps"}, MaxObjects: 20, RequestTimeout: time.Second, RunTimeout: 2 * time.Second, CheckInterval: 30 * time.Millisecond, WatchTimeout: time.Minute, Policy: health.DefaultPolicy()}
}
func clientFor(t *testing.T, server *httptest.Server) dynamic.Interface {
	t.Helper()
	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func until(t *testing.T, f func() bool) {
	t.Helper()
	timer := time.NewTimer(4 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if f() {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("condition timed out")
		case <-tick.C:
		}
	}
}

func TestInformerUpdatesDeletionAndRecovery(t *testing.T) {
	var mu sync.Mutex
	streams := map[string]chan map[string]any{}
	pods := []any{}
	listCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("cluster write attempted")
			w.WriteHeader(405)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/nodes") && !strings.Contains(r.URL.Path, "/namespaces/apps/") {
			t.Error("request escaped namespace scope")
		}
		if strings.HasSuffix(r.URL.Path, "/events") && r.URL.Query().Get("fieldSelector") != "type=Warning" {
			t.Error("events not filtered")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			ch := make(chan map[string]any, 10)
			mu.Lock()
			streams[r.URL.Path] = ch
			mu.Unlock()
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			for {
				select {
				case <-r.Context().Done():
					return
				case event := <-ch:
					if event == nil {
						return
					}
					if err := json.NewEncoder(w).Encode(event); err != nil {
						return
					}
					w.(http.Flusher).Flush()
				}
			}
		}
		mu.Lock()
		defer mu.Unlock()
		listCalls++
		items := []any{}
		kind := "List"
		if strings.HasSuffix(r.URL.Path, "/nodes") {
			items = append(items, node())
			kind = "NodeList"
		}
		if strings.HasSuffix(r.URL.Path, "/pods") {
			items = append(items, pods...)
			kind = "PodList"
		}
		json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": kind, "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	defer server.Close()
	s := NewSource(clientFor(t, server), testConfig())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	defer func() { cancel(); <-done }()
	until(t, func() bool {
		_, err := s.Snapshot(time.Now())
		mu.Lock()
		defer mu.Unlock()
		return err == nil && len(streams) == 5
	})
	path := "/api/v1/namespaces/apps/pods"
	mu.Lock()
	stream := streams[path]
	mu.Unlock()
	bad := pod("bad", "ErrImagePull")
	stream <- map[string]any{"type": "ADDED", "object": bad}
	until(t, func() bool {
		snapshot, err := s.Snapshot(time.Now())
		return err == nil && health.Status(health.Evaluate(snapshot, testConfig().Policy)) == "unhealthy"
	})
	snapshot, _ := s.Snapshot(time.Now())
	data, _ := json.Marshal(snapshot)
	if strings.Contains(string(data), "must-not-retain") || strings.Contains(string(data), "registry/app:stale") {
		t.Fatal("cache retained raw or stale evidence")
	}
	stream <- map[string]any{"type": "DELETED", "object": bad}
	until(t, func() bool { snapshot, err := s.Snapshot(time.Now()); return err == nil && len(snapshot.Objects) == 1 })
	mu.Lock()
	pods = []any{bad}
	before := listCalls
	mu.Unlock()
	stream <- map[string]any{"type": "ERROR", "object": map[string]any{"apiVersion": "v1", "kind": "Status", "code": 410, "reason": "Expired", "message": "raw-secret-error"}}
	until(t, func() bool { _, err := s.Snapshot(time.Now()); return err != nil })
	until(t, func() bool {
		snapshot, err := s.Snapshot(time.Now())
		mu.Lock()
		defer mu.Unlock()
		return err == nil && len(snapshot.Objects) == 2 && listCalls >= before+5
	})
	// A hanging stream is eventually closed by the configured watch deadline.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watch shutdown blocked")
	}
}

func TestCollectionBoundsPaginationAndErrors(t *testing.T) {
	for _, name := range []string{"pagination", "shared budget", "empty nodes", "metadata missing", "repeated token", "expired token", "permission", "duplicate objects"} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if name == "permission" {
					w.WriteHeader(403)
					fmt.Fprint(w, `{"message":"do not leak me"}`)
					return
				}
				if name == "expired token" && r.URL.Query().Get("continue") != "" {
					w.WriteHeader(410)
					return
				}
				meta := map[string]any{"resourceVersion": "1"}
				items := []any{}
				if strings.HasSuffix(r.URL.Path, "/nodes") {
					if name != "empty nodes" {
						items = []any{node()}
					}
					if name == "metadata missing" {
						delete(meta, "resourceVersion")
					}
					if name == "pagination" || name == "expired token" {
						if r.URL.Query().Get("continue") == "" {
							meta["continue"] = "page2"
						} else {
							n := node()
							n["metadata"] = map[string]any{"name": "node2", "uid": "node2", "resourceVersion": "1"}
							items = []any{n}
						}
					}
					if name == "repeated token" {
						meta["continue"] = "same"
					}
					if name == "duplicate objects" {
						items = append(items, node())
					}
				}
				if strings.HasSuffix(r.URL.Path, "/pods") && name == "shared budget" {
					items = []any{pod("one", "ErrImagePull")}
				}
				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": meta, "items": items})
			}))
			defer server.Close()
			c := testConfig()
			if name == "shared budget" {
				c.MaxObjects = 1
			}
			s := NewSource(clientFor(t, server), c)
			snapshot, err := s.Collect(t.Context(), time.Now())
			if name == "pagination" {
				if err != nil || len(snapshot.Objects) != 2 {
					t.Fatalf("snapshot=%+v err=%v", snapshot, err)
				}
			} else if err == nil {
				t.Fatal("incomplete collection accepted")
			}
			if err != nil && strings.Contains(err.Error(), "do not leak me") {
				t.Fatal("API error leaked")
			}
		})
	}
}

func TestProjectionRejectsUnexpectedScopeAndIdentity(t *testing.T) {
	p := pod("pod", "ErrImagePull")
	u := &unstructured.Unstructured{Object: p}
	if _, err := project(u, "Pod", "other"); err == nil {
		t.Fatal("wrong namespace accepted")
	}
	u.SetUID("")
	if _, err := project(u, "Pod", "apps"); err == nil {
		t.Fatal("missing UID accepted")
	}
}

func TestBudgetDeleteReleasesCapacity(t *testing.T) {
	b := &budget{max: 1, keys: map[string]struct{}{}}
	if b.admit("a") != nil || b.admit("a") != nil {
		t.Fatal("updates consumed capacity")
	}
	if b.admit("b") == nil {
		t.Fatal("budget was not enforced")
	}
	b.remove("a")
	if b.admit("b") != nil {
		t.Fatal("deletion did not free capacity")
	}
}

func TestWatchBudgetAndDeadlineInvalidateCollection(t *testing.T) {
	for _, mode := range []string{"budget", "deadline", "permission"} {
		t.Run(mode, func(t *testing.T) {
			trigger := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Query().Get("watch") == "true" {
					if mode == "permission" {
						w.WriteHeader(403)
						return
					}
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					if mode == "budget" && strings.HasSuffix(r.URL.Path, "/pods") {
						select {
						case <-trigger:
						case <-r.Context().Done():
							return
						}
						json.NewEncoder(w).Encode(map[string]any{"type": "ADDED", "object": pod("excess", "ErrImagePull")})
						w.(http.Flusher).Flush()
					}
					<-r.Context().Done()
					return
				}
				items := []any{}
				if strings.HasSuffix(r.URL.Path, "/nodes") {
					items = append(items, node())
				}
				json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "List", "metadata": map[string]any{"resourceVersion": "1"}, "items": items})
			}))
			defer server.Close()
			c := testConfig()
			if mode == "budget" {
				c.MaxObjects = 1
			}
			if mode == "deadline" {
				c.WatchTimeout = 300 * time.Millisecond
				c.RequestTimeout = 100 * time.Millisecond
			}
			source := NewSource(clientFor(t, server), c)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() { defer close(done); source.Run(ctx) }()
			defer func() { cancel(); <-done }()
			if mode != "permission" {
				until(t, func() bool { return source.Err() == nil })
				close(trigger)
			}
			until(t, func() bool {
				err := source.Err()
				if err == nil {
					return false
				}
				switch mode {
				case "budget":
					return strings.Contains(err.Error(), "budget")
				case "deadline":
					return strings.Contains(err.Error(), "deadline") || strings.Contains(err.Error(), "watch ended")
				default:
					return strings.Contains(err.Error(), "watch")
				}
			})
			if _, err := source.Snapshot(time.Now()); err == nil {
				t.Fatal("failed watcher returned complete collection")
			}
		})
	}
}

func TestProjectionOfDeploymentJobAndEvent(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Second)
	metadata := map[string]any{"name": "test", "namespace": "apps", "uid": "test-id", "resourceVersion": "1", "creationTimestamp": at.Format(time.RFC3339)}
	for _, test := range []struct {
		kind   string
		fields map[string]any
	}{
		{"Deployment", map[string]any{"spec": map[string]any{"replicas": int64(2)}, "status": map[string]any{"observedGeneration": int64(3), "availableReplicas": int64(1), "updatedReplicas": int64(2)}}},
		{"Job", map[string]any{"status": map[string]any{"conditions": []any{map[string]any{"type": "Failed", "status": "True", "reason": "DeadlineExceeded", "message": "not-allowed"}}}}},
		{"Event", map[string]any{"type": "Warning", "message": "not-allowed", "involvedObject": map[string]any{"kind": "Pod", "namespace": "apps", "name": "pod", "uid": "pod-id"}}},
	} {
		t.Run(test.kind, func(t *testing.T) {
			u := &unstructured.Unstructured{Object: test.fields}
			u.Object["metadata"] = metadata
			value, err := project(u, test.kind, "apps")
			if err != nil {
				t.Fatal(err)
			}
			if test.kind == "Event" && !value.Value.EventTime.IsZero() {
				t.Fatal("creation time substituted for an event occurrence")
			}
			if test.kind == "Deployment" && (value.Value.Desired != 2 || value.Value.Observed != 3) {
				t.Fatal(value.Value)
			}
			data, _ := json.Marshal(value)
			if strings.Contains(string(data), "not-allowed") {
				t.Fatal("raw message retained")
			}
		})
	}
}

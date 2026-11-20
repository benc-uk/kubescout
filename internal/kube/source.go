/*
Purpose: Collect scoped Kubernetes evidence through bounded lists and watches.
Flow: Cluster-scoped Nodes/PVs/attachments, namespace filters and shared admission limits feed one-shot
snapshots or informer caches with change notifications. Broken or expired
watches invalidate and rebuild caches; labelled progress describes collection only.
Collection errors remain unknown
observations for the runner, never healthy evidence or synthetic incidents.
Epoch generations and bounded per-name change history preserve recovery resets across coalesced notifications.
Configured extra resources use the same scoped dynamic lists/watches and admission/freshness boundaries.
Startup diagnostics enumerate the configured collection plan without reading cluster objects.
*/
package kube

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benc-uk/kubescout/internal/config"
	"github.com/benc-uk/kubescout/internal/health"
	"github.com/benc-uk/kubescout/internal/output"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

type resource struct {
	gvr             schema.GroupVersionResource
	kind, namespace string
}

type budget struct {
	mu   sync.Mutex
	keys map[string]struct{}
	max  int
}

type Source struct {
	Log        *output.Writer
	client     dynamic.Interface
	config     config.Config
	mu         sync.RWMutex
	current    *epoch
	err        error
	changes    chan struct{}
	generation atomic.Uint64
}

type epoch struct {
	mu               sync.RWMutex
	err              error
	ready            bool
	informers        []cache.SharedIndexInformer
	failed           chan struct{}
	once             sync.Once
	budget           *budget
	wg               sync.WaitGroup
	generation       uint64
	recoverySequence uint64
	recovery         map[health.Resource]recoveryEntry
	generations      *atomic.Uint64
}

type boundedListWatch struct{ *cache.ListWatch }

type projectedWatch struct {
	cancel context.CancelFunc
	events chan watch.Event
}

// resources selects built-in and configured resource scopes, sharing namespace filters.
// All/ignore mode uses all-namespace endpoints without resource discovery.
func resources(c config.Config) []resource {
	out := []resource{
		{schema.GroupVersionResource{Version: "v1", Resource: "nodes"}, "Node", ""},
		{schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}, "PersistentVolume", ""},
		{schema.GroupVersionResource{Group: "storage.k8s.io", Version: "v1", Resource: "volumeattachments"}, "VolumeAttachment", ""},
	}

	namespaces := c.NamespaceWatch
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}

	for _, ns := range namespaces {
		out = append(out,
			resource{schema.GroupVersionResource{Version: "v1", Resource: "pods"}, "Pod", ns},
			resource{schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}, "PersistentVolumeClaim", ns},
			resource{schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, "Deployment", ns},
			resource{schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}, "ReplicaSet", ns},
			resource{schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}, "StatefulSet", ns},
			resource{schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}, "DaemonSet", ns},
			resource{schema.GroupVersionResource{Version: "v1", Resource: "services"}, "Service", ns},
			resource{schema.GroupVersionResource{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"}, "EndpointSlice", ns},
			resource{schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}, "Job", ns},
			resource{schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "cronjobs"}, "CronJob", ns},
			resource{schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}, "HorizontalPodAutoscaler", ns},
			resource{schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"}, "Ingress", ns},
			resource{schema.GroupVersionResource{Version: "v1", Resource: "events"}, "Event", ns})
	}

	for i := range c.Policy.ExtraResources {
		r := &c.Policy.ExtraResources[i]

		scopes := namespaces
		if r.Scope == "Cluster" {
			scopes = []string{""}
		}

		for _, ns := range scopes {
			out = append(out, resource{schema.GroupVersionResource{Group: r.Group, Version: r.Version, Resource: r.Resource}, r.Kind, ns})
		}
	}

	return out
}

// admit reserves shared capacity for a new object key under lock; existing keys consume no extra capacity.
func (b *budget) admit(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, found := b.keys[key]; found {
		return nil
	}

	if len(b.keys) >= b.max {
		return errors.New("shared object budget exceeded")
	}

	b.keys[key] = struct{}{}

	return nil
}

// remove releases an object's shared admission capacity under lock.
func (b *budget) remove(key string) { b.mu.Lock(); delete(b.keys, key); b.mu.Unlock() }

// NewSource initializes an unsynchronized collector with a coalescing change channel but starts no API requests.
func NewSource(client dynamic.Interface, c config.Config) *Source {
	return &Source{client: client, config: c, err: errors.New("initial cache synchronisation pending"), changes: make(chan struct{}, 1)}
}

// LogPlan describes the actual configured resource inventory without making Kubernetes requests.
func (s *Source) LogPlan() {
	plan := resources(s.config)
	kinds := []string{}

	for _, resource := range plan {
		if !slices.Contains(kinds, resource.kind) {
			kinds = append(kinds, resource.kind)
		}
	}

	_ = s.Log.Log("info", "GO", "COLLECT", fmt.Sprintf("Collection plan: %d resource/scope pairs; types=%s; Events restricted to Warning", len(plan), strings.Join(kinds, ", ")))
}

// Changes exposes coalesced notifications for cache updates, readiness and collection failures.
func (s *Source) Changes() <-chan struct{} { return s.changes }

// Err reports source or current-epoch failures and rejects caches whose initial synchronization is pending.
func (s *Source) Err() error {
	s.mu.RLock()
	e, err := s.current, s.err
	s.mu.RUnlock()

	if err != nil {
		return err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.err != nil {
		return e.err
	}

	if !e.ready {
		return errors.New("cache synchronisation pending")
	}

	return nil
}

// notify queues a change signal without blocking, coalescing notifications while one is already pending.
func (s *Source) notify() {
	select {
	case s.changes <- struct{}{}:
	default:
	}
}

// Snapshot reads the current synchronized cache, rejecting failed epochs, missing Nodes or excess objects.
// The supplied time labels the observation; initial synchronization alone does not establish freshness.
func (s *Source) Snapshot(now time.Time) (health.Snapshot, error) {
	s.mu.RLock()
	e, err := s.current, s.err
	s.mu.RUnlock()

	if err != nil {
		return health.Snapshot{}, err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.err != nil {
		return health.Snapshot{}, e.err
	}

	if !e.ready {
		return health.Snapshot{}, errors.New("cache synchronisation pending")
	}

	snapshot := health.Snapshot{At: now, Continuity: e.generation, RecoveryChanges: map[health.Resource]uint64{}}
	for name, entry := range e.recovery {
		snapshot.RecoveryChanges[name] = entry.Change
	}

	nodes := 0

	for _, informer := range e.informers {
		for _, raw := range informer.GetStore().List() {
			obj := raw.(*cachedObject)

			snapshot.Objects = append(snapshot.Objects, obj.Value)
			if obj.Value.Kind == "Node" {
				nodes++
			}
		}
	}

	if nodes == 0 {
		return health.Snapshot{}, errors.New("no nodes observed")
	}

	if len(snapshot.Objects) > s.config.MaxObjects {
		return health.Snapshot{}, errors.New("shared object budget exceeded")
	}

	return snapshot, nil
}

// Run rebuilds every scoped cache after a broken/expired watch. HasSynced alone
// only describes initial sync and cannot prove a disconnected cache is current.
func (s *Source) Run(ctx context.Context) {
	initialDelay := min(s.config.CheckInterval, time.Second)
	delay := initialDelay

	for ctx.Err() == nil {
		if s.runEpoch(ctx) {
			delay = initialDelay
		}

		if ctx.Err() != nil {
			break
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		delay = min(2*delay, 30*time.Second)
	}
}

// runEpoch installs and runs all scoped informers with one shared budget until failure or cancellation.
// It notifies readiness or failure, waits for owned workers to stop and reports whether initial sync succeeded.
func (s *Source) runEpoch(parent context.Context) bool {
	_ = s.Log.Log("info", "GO", "COLLECT", "Synchronising scoped resource caches")
	ctx, cancel := context.WithCancel(parent)
	e := &epoch{failed: make(chan struct{}), budget: &budget{keys: map[string]struct{}{}, max: s.config.MaxObjects},
		recovery: map[health.Resource]recoveryEntry{}, generations: &s.generation}
	fail := func(err error) {
		e.once.Do(func() {
			e.mu.Lock()
			e.err = err
			e.mu.Unlock()
			close(e.failed)
			s.notify()
			cancel()
		})
	}

	for _, r := range resources(s.config) {
		lw := &cache.ListWatch{
			DisableChunking: true,
			ListWithContextFunc: func(callCtx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
				list, err := s.list(callCtx, r, e.budget)
				if err == nil {
					for _, obj := range list.Items {
						if err = e.observeRecovery(obj.Value, false, true, s.config.MaxObjects); err != nil {
							break
						}
					}
				}

				if err != nil {
					fail(err)
				}

				return list, err
			},
			WatchFuncWithContext: func(callCtx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
				return s.watch(callCtx, r, opts, e, fail)
			},
		}

		informer := cache.NewSharedIndexInformer(boundedListWatch{lw}, &cachedObject{}, 0, cache.Indexers{})
		if err := informer.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, _ error) {
			fail(errors.New("Kubernetes watch failed; rebuilding scoped caches"))
		}); err != nil {
			fail(err)
			break
		}

		if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(any) { s.notify() },
			UpdateFunc: func(any, any) { s.notify() },
			DeleteFunc: func(any) { s.notify() },
		}); err != nil {
			fail(err)
			break
		}

		e.informers = append(e.informers, informer)
	}

	s.mu.Lock()
	e.generation = s.generation.Add(1)
	s.current, s.err = e, nil
	s.mu.Unlock()

	defer func() { cancel(); e.wg.Wait() }()

	for _, informer := range e.informers {
		e.wg.Go(func() { informer.Run(ctx.Done()) })
	}

	syncCtx, stop := context.WithTimeout(ctx, s.config.RunTimeout)
	defer stop()

	synced := make([]cache.InformerSynced, 0, len(e.informers))
	for _, informer := range e.informers {
		synced = append(synced, informer.HasSynced)
	}

	if !cache.WaitForCacheSync(syncCtx.Done(), synced...) {
		fail(errors.New("cache synchronisation failed or timed out"))
		return false
	}

	e.mu.Lock()
	e.ready = true
	e.mu.Unlock()
	s.notify()
	_ = s.Log.Log("info", "GO", "COLLECT", "Scoped caches synchronised; continuing watch freshness checks")

	select {
	case <-ctx.Done():
	case <-e.failed:
	}

	return true
}

// IsWatchListSemanticsUnSupported disables streaming initial lists, which bypass paginated admission limits.
func (boundedListWatch) IsWatchListSemanticsUnSupported() bool { return true }

// list performs deadline-bound pagination, projects in-scope objects and charges the shared budget.
// It rejects inconsistent versions, duplicate identities or tokens and returns sanitized API errors.
func (s *Source) list(ctx context.Context, r resource, b *budget) (*cachedList, error) {
	_ = s.Log.Log("debug", "GO", "COLLECT.LIST", fmt.Sprintf("Listing %s scope=%q", r.kind, r.namespace))
	list := &cachedList{}
	seen := map[string]bool{}
	identities := map[string]bool{}

	opts := metav1.ListOptions{Limit: 100}
	if r.kind == "Event" {
		opts.FieldSelector = "type=Warning"
	}

	for {
		callCtx, cancel := context.WithTimeout(ctx, s.config.RequestTimeout)
		page, err := s.client.Resource(r.gvr).Namespace(r.namespace).List(callCtx, opts)

		cancel()

		if err != nil {
			return nil, fmt.Errorf("%s (%s) list failed in scope %q", r.kind, r.gvr.String(), r.namespace)
		}

		if page.GetResourceVersion() == "" {
			return nil, errors.New("list metadata lacks resourceVersion")
		}

		if list.ResourceVersion != "" && list.ResourceVersion != page.GetResourceVersion() {
			return nil, errors.New("pagination changed resourceVersion")
		}

		list.ResourceVersion = page.GetResourceVersion()
		for i := range page.Items {
			obj, err := s.project(&page.Items[i], r)
			if err != nil {
				return nil, err
			}

			if obj == nil {
				continue
			}

			key := objectKey(obj)
			if identities[key] {
				return nil, errors.New("duplicate resource in paginated list")
			}

			identities[key] = true

			if err := b.admit(objectKey(obj)); err != nil {
				return nil, err
			}

			list.Items = append(list.Items, *obj)
			if len(list.Items) > s.config.MaxObjects {
				return nil, errors.New("duplicate or excessive list objects")
			}
		}

		token := page.GetContinue()
		if token == "" {
			_ = s.Log.Log("debug", "GO", "COLLECT.LIST", fmt.Sprintf("%s scope=%q: %d admitted objects", r.kind, r.namespace, len(list.Items)))
			return list, nil
		}

		if seen[token] {
			return nil, errors.New("repeated pagination token")
		}

		seen[token] = true
		opts.Continue = token
	}
}

// project enforces cluster or requested namespace scope and filters excluded namespaces before projection.
// A nil object without an error means the namespace was intentionally skipped.
func (s *Source) project(u *unstructured.Unstructured, r resource) (*cachedObject, error) {
	if extra, ok := s.config.ExtraResource(r.gvr.Group, r.kind); ok {
		namespace := u.GetNamespace()
		if extra.Scope == "Cluster" {
			if namespace != "" {
				return nil, errors.New("cluster-scoped extra resource has a namespace")
			}
		} else {
			if len(validation.IsDNS1123Label(namespace)) != 0 || r.namespace != "" && namespace != r.namespace {
				return nil, errors.New("extra resource outside requested namespace scope")
			}

			if !s.config.WatchesNamespace(namespace) {
				return nil, nil
			}
		}

		value, err := ProjectExtraResource(u, *extra, namespace, s.config.APIKey)
		if err != nil {
			return nil, err
		}

		return &cachedObject{ObjectMeta: metav1.ObjectMeta{Name: value.Name, Namespace: namespace,
			UID: u.GetUID(), ResourceVersion: value.Version}, Value: value}, nil
	}

	if health.ClusterScoped(r.kind) {
		return project(u, r.kind, "")
	}

	namespace := u.GetNamespace()
	if len(validation.IsDNS1123Label(namespace)) != 0 || r.namespace != "" && namespace != r.namespace {
		return nil, errors.New("missing resource namespace or response outside requested scope")
	}

	if !s.config.WatchesNamespace(namespace) {
		return nil, nil
	}

	return project(u, r.kind, namespace)
}

// objectKey identifies a kind/namespace/name slot for duplicate detection and shared admission accounting.
func objectKey(obj *cachedObject) string {
	return obj.Value.APIGroup + "/" + obj.Value.Kind + "/" + obj.Namespace + "/" + obj.Name
}

// watch opens a deadline-bound stream and forwards only scoped, projected resource events.
// Its epoch-owned worker updates admission capacity and invalidates the epoch on stream or projection failure.
func (s *Source) watch(ctx context.Context, r resource, opts metav1.ListOptions, e *epoch, fail func(error)) (watch.Interface, error) {
	watchCtx, cancel := context.WithTimeout(ctx, s.config.WatchTimeout+s.config.RequestTimeout)
	seconds := int64(s.config.WatchTimeout / time.Second)

	opts.TimeoutSeconds, opts.AllowWatchBookmarks = &seconds, true
	if r.kind == "Event" {
		opts.FieldSelector = "type=Warning"
	}
	// A bounded opening timer also covers a server that never sends headers.
	timer := time.AfterFunc(s.config.RequestTimeout, cancel)
	upstream, err := s.client.Resource(r.gvr).Namespace(r.namespace).Watch(watchCtx, opts)

	timer.Stop()

	if err != nil {
		cancel()

		err = fmt.Errorf("%s (%s) watch could not start in scope %q", r.kind, r.gvr.String(), r.namespace)
		fail(err)

		return nil, err
	}

	_ = s.Log.Log("debug", "GO", "COLLECT.WATCH", fmt.Sprintf("Watching %s scope=%q", r.kind, r.namespace))
	result := &projectedWatch{cancel: cancel, events: make(chan watch.Event)}

	e.wg.Go(func() {
		defer close(result.events)
		defer cancel()
		defer upstream.Stop()
		defer func() {
			if ctx.Err() == nil {
				fail(errors.New("watch ended; rebuilding scoped caches"))
			}
		}()

		for {
			select {
			case <-watchCtx.Done():
				if ctx.Err() == nil {
					fail(errors.New("watch deadline reached; rebuilding scoped caches"))
				}

				return
			case event, ok := <-upstream.ResultChan():
				if !ok || event.Type == watch.Error {
					if ctx.Err() == nil {
						fail(errors.New("watch ended or expired; rebuilding scoped caches"))
					}

					return
				}

				if event.Type == watch.Bookmark {
					continue
				}

				u, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					fail(errors.New("unexpected watch object"))
					return
				}

				if event.Type != watch.Added && event.Type != watch.Modified && event.Type != watch.Deleted {
					fail(errors.New("unexpected watch event type"))
					return
				}

				obj, err := s.project(u, r)
				if err != nil {
					fail(err)
					return
				}

				if obj == nil {
					continue
				}

				switch event.Type {
				case watch.Added, watch.Modified:
					if err := e.budget.admit(objectKey(obj)); err != nil {
						fail(err)
						return
					}
				case watch.Deleted:
					e.budget.remove(objectKey(obj))
				default:
					fail(errors.New("unexpected watch event type"))
					return
				}

				if err := e.observeRecovery(obj.Value, event.Type == watch.Deleted, false, s.config.MaxObjects); err != nil {
					fail(err)
					return
				}

				select {
				case result.events <- watch.Event{Type: event.Type, Object: obj}:
				case <-watchCtx.Done():
					return
				}
			}
		}
	})

	return result, nil
}

// Stop cancels the projected stream so its worker closes the event channel and stops the upstream watch.
func (w *projectedWatch) Stop() { w.cancel() }

// ResultChan exposes projected resource events rather than raw upstream objects.
func (w *projectedWatch) ResultChan() <-chan watch.Event { return w.events }

// Collect lists every configured resource scope under one shared budget without starting watches.
// Any list failure or absence of Nodes rejects the entire one-shot observation.
func (s *Source) Collect(ctx context.Context, now time.Time) (health.Snapshot, error) {
	b := &budget{keys: map[string]struct{}{}, max: s.config.MaxObjects}
	snapshot := health.Snapshot{At: now}

	for _, r := range resources(s.config) {
		list, err := s.list(ctx, r, b)
		if err != nil {
			return health.Snapshot{}, err
		}

		if r.kind == "Node" && len(list.Items) == 0 {
			return health.Snapshot{}, errors.New("no nodes observed")
		}

		for _, obj := range list.Items {
			snapshot.Objects = append(snapshot.Objects, obj.Value)
		}
	}

	return snapshot, nil
}

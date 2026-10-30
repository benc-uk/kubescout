//go:build integration

package cluster

import (
	"context"
	"crypto/subtle"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"k8s-ops-agent/internal/report"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const (
	proxyPass int32 = iota
	proxyClosed
	proxySilent
)

// The proxy can only read this test's scope. The observer gets an ephemeral
// loopback token, never a copy of the user's Kubernetes credentials.
type faultProxy struct {
	mode           atomic.Int32
	lists, watches atomic.Int64
	mu             sync.Mutex
	active         map[*http.Request]context.CancelFunc
	path           string
}

func (f *fixture) proxy(mode int32) *faultProxy {
	f.t.Helper()
	p := &faultProxy{active: map[*http.Request]context.CancelFunc{}}
	p.mode.Store(mode)
	target, err := url.Parse(f.config.Host)
	if err != nil {
		f.t.Fatal("invalid API address")
	}
	rc := rest.CopyConfig(f.config)
	rc.Timeout = 0
	transport, err := rest.TransportFor(rc)
	if err != nil {
		f.t.Fatal("cannot construct authenticated API proxy transport")
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = transport
	forward.FlushInterval = -1
	forward.ErrorLog = log.New(io.Discard, "", 0)
	forward.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "test API proxy unavailable", http.StatusBadGateway)
	}
	token := report.ID()
	allowed := map[string]bool{
		"/api/v1/nodes": true,
		"/api/v1/namespaces/" + f.namespace + "/pods":              true,
		"/api/v1/namespaces/" + f.namespace + "/events":            true,
		"/apis/apps/v1/namespaces/" + f.namespace + "/deployments": true,
		"/apis/batch/v1/namespaces/" + f.namespace + "/jobs":       true,
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 || r.Method != http.MethodGet || !allowed[r.URL.Path] {
			http.Error(w, "not authorised", http.StatusForbidden)
			return
		}
		if p.mode.Load() == proxyClosed {
			http.Error(w, "test connection interruption", http.StatusServiceUnavailable)
			return
		}
		isWatch := r.URL.Query().Get("watch") == "true"
		if isWatch {
			p.watches.Add(1)
		} else {
			p.lists.Add(1)
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		requestKey := r
		p.mu.Lock()
		p.active[requestKey] = cancel
		p.mu.Unlock()
		defer func() { p.mu.Lock(); delete(p.active, requestKey); p.mu.Unlock() }()
		if isWatch && p.mode.Load() == proxySilent {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-ctx.Done()
			return
		}
		r = r.Clone(ctx)
		r.Header.Del("Authorization")
		forward.ServeHTTP(w, r)
	}))
	f.t.Cleanup(server.Close)
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["proxy"] = &clientcmdapi.Cluster{
		Server:                   server.URL,
		CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
	}
	cfg.AuthInfos["proxy"] = &clientcmdapi.AuthInfo{Token: token}
	cfg.Contexts["stage2-proxy"] = &clientcmdapi.Context{Cluster: "proxy", AuthInfo: "proxy"}
	cfg.CurrentContext = "stage2-proxy"
	data, err := clientcmd.Write(*cfg)
	if err != nil {
		f.t.Fatal("cannot encode test proxy configuration")
	}
	p.path = filepath.Join(f.t.TempDir(), "proxy-kubeconfig")
	if err := os.WriteFile(p.path, data, 0600); err != nil {
		f.t.Fatal(err)
	}
	return p
}

func (p *faultProxy) disconnect() {
	p.mode.Store(proxyClosed)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cancel := range p.active {
		cancel()
	}
}

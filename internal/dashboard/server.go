package dashboard

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

//go:embed web/*
var assets embed.FS

func Handler(source Source, logger *log.Logger) http.Handler {
	mux := http.NewServeMux()
	writeError := func(w http.ResponseWriter, err error) {
		var failure *APIError
		if !errors.As(err, &failure) {
			failure = &APIError{500, "internal_error", "Dashboard request failed."}
		}
		logger.Printf("ERROR: %s: %s", failure.Code, failure.Message)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(failure.Status)
		if err := json.NewEncoder(w).Encode(map[string]any{"error": failure}); err != nil {
			logger.Print("ERROR: could not write API error response")
		}
	}
	writeJSON := func(w http.ResponseWriter, result any, err error) {
		if err != nil {
			writeError(w, err)
			return
		}
		data, err := json.Marshal(result)
		if err != nil {
			writeError(w, err)
			return
		}
		if len(data) > MaxResponseBytes {
			writeError(w, errResponseSize)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(data); err != nil {
			logger.Print("ERROR: could not write API response")
		}
	}
	mux.HandleFunc("/api/issues", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeError(w, &APIError{405, "method_not_allowed", "Only GET is supported."})
			return
		}
		result, err := source.List(r.Context())
		writeJSON(w, result, err)
	})
	mux.HandleFunc("/api/issues/{name}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			writeError(w, &APIError{405, "method_not_allowed", "Only GET is supported."})
			return
		}
		result, err := source.Get(r.Context(), r.PathValue("name"))
		writeJSON(w, result, err)
	})
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, &APIError{404, "not_found", "Unknown API route."})
	})
	for _, asset := range []struct{ path, file, contentType string }{
		{"/", "index.html", "text/html; charset=utf-8"},
		{"/styles.css", "styles.css", "text/css; charset=utf-8"},
		{"/app.js", "app.js", "text/javascript; charset=utf-8"},
		{"/model.js", "model.js", "text/javascript; charset=utf-8"},
	} {
		data, err := assets.ReadFile("web/" + asset.file)
		if err != nil {
			panic("missing embedded dashboard asset: " + asset.file)
		}
		mux.HandleFunc(asset.path, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != asset.path {
				http.NotFound(w, r)
				return
			}
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.Header().Set("Allow", "GET, HEAD")
				http.Error(w, "Only GET and HEAD are supported.", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", asset.contentType)
			http.ServeContent(w, r, asset.file, time.Time{}, bytes.NewReader(data))
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; img-src data:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		if r.URL.RawQuery != "" {
			writeError(w, &APIError{400, "invalid_query", "Query parameters are not supported; the issue namespace is configured on the server."})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func Serve(ctx context.Context, listener net.Listener, handler http.Handler, diagnostics io.Writer) error {
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
		ErrorLog:    log.New(diagnostics, "HTTP: ", 0),
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	select {
	case err := <-stopped:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			closeErr := server.Close()
			return errors.Join(err, closeErr)
		}
		err := <-stopped
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

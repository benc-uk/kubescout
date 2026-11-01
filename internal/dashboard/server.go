/*
Purpose: Serve the standalone dashboard's embedded UI and bounded HTTP API.
Flow: Issue reads and same-origin, typed human-triage requests reach a Source;
responses use bounded JSON and browser security headers. Listener lifecycle
supports graceful shutdown. This service does not run the AI investigation
loop or expose incident-spec writes.
*/
package dashboard

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"time"
)

//go:embed web/*
var assets embed.FS

// Handler serves embedded assets and bounded issue APIs with browser security headers and error logging.
// Human triage requires same-origin checks and typed input; no AI or incident-spec writes are exposed.
func Handler(source Source, logger *log.Logger) http.Handler {
	mux := http.NewServeMux()
	origins := http.NewCrossOriginProtection()
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
	mux.HandleFunc("/api/issues/{name}/triage", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeError(w, &APIError{405, "method_not_allowed", "Only POST is supported."})

			return
		}

		if origins.Check(r) != nil || r.Header.Get("X-KubeScout-Action") != "triage" {
			writeError(w, &APIError{403, "invalid_origin", "Triage changes require a same-origin dashboard request."})
			return
		}

		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeError(w, &APIError{415, "invalid_content_type", "Triage changes require application/json."})
			return
		}

		version, dismissed, err := decodeTriage(http.MaxBytesReader(w, r.Body, 1024))
		if err != nil {
			writeError(w, &APIError{400, "invalid_triage", "Expected only an issue version and a dismissed boolean."})
			return
		}

		result, err := source.SetDismissed(r.Context(), r.PathValue("name"), version, dismissed)
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
		{"/logo.png", "logo.png", "image/png"},
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
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")

		if r.URL.RawQuery != "" {
			writeError(w, &APIError{400, "invalid_query", "Query parameters are not supported; the issue namespace is configured on the server."})
			return
		}

		mux.ServeHTTP(w, r)
	})
}

// decodeTriage accepts only a nonempty version and a dismissed boolean, rejecting duplicate or trailing data.
func decodeTriage(body io.Reader) (string, bool, error) {
	decoder := json.NewDecoder(body)

	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return "", false, errors.New("expected object")
	}

	seen := map[string]bool{}

	var (
		version   string
		dismissed *bool
	)

	for decoder.More() {
		token, err := decoder.Token()

		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return "", false, errors.New("invalid or duplicate field")
		}

		seen[key] = true
		switch key {
		case "version":
			err = decoder.Decode(&version)
		case "dismissed":
			err = decoder.Decode(&dismissed)
		default:
			return "", false, errors.New("unknown field")
		}

		if err != nil {
			return "", false, err
		}
	}

	if _, err := decoder.Token(); err != nil {
		return "", false, err
	}

	if err := decoder.Decode(new(any)); err != io.EOF || version == "" || dismissed == nil {
		return "", false, errors.New("missing fields or trailing data")
	}

	return version, *dismissed, nil
}

// Serve runs HTTP with bounded connection timeouts and logs transport errors to diagnostics.
// Cancellation attempts graceful shutdown, then closes remaining connections if shutdown fails.
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

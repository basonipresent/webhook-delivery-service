// Package api exposes the webhook service over HTTP/JSON.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"webhook-delivery-service/internal/store"
)

const (
	maxBodyBytes    = 1 << 20
	minSecretLength = 16
)

type server struct {
	store  *store.Store
	now    func() time.Time
	logger *slog.Logger
}

// New returns the HTTP handler. now defaults to time.Now and logger to
// slog.Default() when nil.
func New(st *store.Store, now func() time.Time, logger *slog.Logger) http.Handler {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	s := &server{store: st, now: now, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /endpoints", s.createEndpoint)
	mux.HandleFunc("POST /events", s.createEvent)
	mux.HandleFunc("GET /events/{id}", s.getEvent)
	mux.HandleFunc("GET /deliveries/{id}", s.getDelivery)
	mux.HandleFunc("POST /deliveries/{id}/redeliver", s.redeliver)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// --- request/response shapes ---

type createEndpointRequest struct {
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
	Secret     string   `json:"secret"`
}

type endpointResponse struct {
	ID         string    `json:"id"`
	URL        string    `json:"url"`
	EventTypes []string  `json:"event_types"`
	CreatedAt  time.Time `json:"created_at"`
}

type createEventRequest struct {
	EventID   string          `json:"event_id"`
	Type      string          `json:"type"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

type createEventResponse struct {
	EventID    string            `json:"event_id"`
	Deliveries []deliverySummary `json:"deliveries"`
}

type deliverySummary struct {
	ID         string       `json:"id"`
	EndpointID string       `json:"endpoint_id"`
	Status     store.Status `json:"status"`
}

type eventResponse struct {
	EventID    string             `json:"event_id"`
	Type       string             `json:"type"`
	CreatedAt  time.Time          `json:"created_at"`
	ReceivedAt time.Time          `json:"received_at"`
	Payload    json.RawMessage    `json:"payload"`
	Deliveries []deliveryResponse `json:"deliveries"`
}

type deliveryResponse struct {
	ID            string            `json:"id"`
	EventID       string            `json:"event_id"`
	EndpointID    string            `json:"endpoint_id"`
	Status        store.Status      `json:"status"`
	NextAttemptAt *time.Time        `json:"next_attempt_at"` // null once terminal
	Attempts      []attemptResponse `json:"attempts"`
}

type attemptResponse struct {
	Number     int       `json:"number"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS int64     `json:"duration_ms"`
	StatusCode int       `json:"status_code"` // 0 = no response
	Error      string    `json:"error"`
	Outcome    string    `json:"outcome"`
}

// --- handlers ---

func (s *server) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req createEndpointRequest
	if !s.decode(w, r, &req) {
		return
	}
	if err := validateEndpoint(req); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ep, err := s.store.CreateEndpoint(store.Endpoint{
		URL:        req.URL,
		Secret:     req.Secret,
		EventTypes: req.EventTypes,
		CreatedAt:  s.now().UTC(),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.writeJSON(w, http.StatusCreated, endpointResponse{
		ID: ep.ID, URL: ep.URL, EventTypes: ep.EventTypes, CreatedAt: ep.CreatedAt,
	})
}

func validateEndpoint(req createEndpointRequest) error {
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("url must be an absolute http or https URL")
	}
	if len(req.EventTypes) == 0 {
		return errors.New("event_types must not be empty")
	}
	for _, t := range req.EventTypes {
		if t == "" {
			return errors.New("event_types must not contain empty strings")
		}
	}
	if len(req.Secret) < minSecretLength {
		return fmt.Errorf("secret must be at least %d bytes", minSecretLength)
	}
	return nil
}

func (s *server) createEvent(w http.ResponseWriter, r *http.Request) {
	var req createEventRequest
	if !s.decode(w, r, &req) {
		return
	}
	createdAt, err := time.Parse(time.RFC3339, req.CreatedAt)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "created_at must be an RFC 3339 timestamp")
		return
	}
	ev, ds, created, err := s.store.CreateEvent(store.Event{
		ID:        req.EventID,
		Type:      req.Type,
		CreatedAt: createdAt,
		Payload:   req.Payload,
	}, s.now().UTC())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	resp := createEventResponse{EventID: ev.ID, Deliveries: make([]deliverySummary, 0, len(ds))}
	for _, d := range ds {
		resp.Deliveries = append(resp.Deliveries, deliverySummary{ID: d.ID, EndpointID: d.EndpointID, Status: d.Status})
	}
	code := http.StatusAccepted
	if !created {
		code = http.StatusOK // identical replay: nothing new was scheduled
	}
	s.writeJSON(w, code, resp)
}

func (s *server) getEvent(w http.ResponseWriter, r *http.Request) {
	ev, ds, err := s.store.GetEvent(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	resp := eventResponse{
		EventID:    ev.ID,
		Type:       ev.Type,
		CreatedAt:  ev.CreatedAt,
		ReceivedAt: ev.ReceivedAt,
		Payload:    ev.Payload,
		Deliveries: make([]deliveryResponse, 0, len(ds)),
	}
	for _, d := range ds {
		resp.Deliveries = append(resp.Deliveries, toDeliveryResponse(d))
	}
	s.writeJSON(w, http.StatusOK, resp)
}

func (s *server) getDelivery(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.GetDelivery(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, toDeliveryResponse(d))
}

// redeliver re-queues a failed delivery with a fresh retry budget, e.g. after
// the merchant fixed their endpoint.
func (s *server) redeliver(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.Redeliver(r.PathValue("id"), s.now().UTC())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.writeJSON(w, http.StatusAccepted, toDeliveryResponse(d))
}

func toDeliveryResponse(d store.Delivery) deliveryResponse {
	resp := deliveryResponse{
		ID:         d.ID,
		EventID:    d.EventID,
		EndpointID: d.EndpointID,
		Status:     d.Status,
		Attempts:   make([]attemptResponse, 0, len(d.Attempts)),
	}
	if !d.NextAttemptAt.IsZero() {
		next := d.NextAttemptAt
		resp.NextAttemptAt = &next
	}
	for _, a := range d.Attempts {
		resp.Attempts = append(resp.Attempts, attemptResponse{
			Number:     a.Number,
			StartedAt:  a.StartedAt,
			DurationMS: a.Duration.Milliseconds(),
			StatusCode: a.StatusCode,
			Error:      a.Error,
			Outcome:    a.Outcome,
		})
	}
	return resp
}

// --- helpers ---

// decode reads a single JSON object from a body capped at maxBodyBytes. On
// failure it writes 400 or 413 and returns false.
func (s *server) decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	err := dec.Decode(dst)
	if err == nil {
		// Reject trailing data such as a second JSON value.
		if extra := dec.Decode(&struct{}{}); !errors.Is(extra, io.EOF) {
			err = errors.New("body must contain a single JSON object")
			if extra != nil {
				err = extra
			}
		}
	}
	if err == nil {
		return true
	}
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		s.writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("body exceeds %d bytes", maxBodyBytes))
		return false
	}
	s.writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
	return false
}

func (s *server) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrInvalid):
		s.writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrNotFound):
		s.writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, store.ErrConflict):
		s.writeError(w, http.StatusConflict, err.Error())
	default:
		s.logger.Error("store operation failed", "err", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func (s *server) writeError(w http.ResponseWriter, code int, msg string) {
	s.writeJSON(w, code, map[string]string{"error": msg})
}

func (s *server) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Warn("write response", "status", code, "err", err)
	}
}

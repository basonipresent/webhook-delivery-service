// Package store holds endpoints, events and deliveries in memory.
// All methods are safe for concurrent use and return deep copies.
package store

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"
)

var (
	ErrInvalid  = errors.New("invalid input")
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

// Endpoint is a merchant URL subscribed to event types. Immutable after create.
type Endpoint struct {
	ID         string
	URL        string
	Secret     string
	EventTypes []string
	CreatedAt  time.Time
}

// Event is an ingested event. Body is the exact JSON sent to every endpoint,
// built once at ingest so signatures cover stable bytes.
type Event struct {
	ID         string
	Type       string
	CreatedAt  time.Time
	ReceivedAt time.Time
	Payload    json.RawMessage
	Body       []byte
}

type Status string

const (
	StatusPending   Status = "pending"
	StatusInFlight  Status = "in_flight"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Delivery is one event sent to one endpoint, with its attempt history.
type Delivery struct {
	ID            string
	EventID       string
	EndpointID    string
	Status        Status
	NextAttemptAt time.Time
	Attempts      []Attempt

	// cycleStart is len(Attempts) when the current retry cycle began. It is 0
	// until a redeliver, which resets the retry budget but keeps the history.
	cycleStart int
}

// Attempt records a single HTTP delivery attempt.
type Attempt struct {
	Number     int
	StartedAt  time.Time
	Duration   time.Duration
	StatusCode int // 0 = no response
	Error      string
	Outcome    string // success | retry | fail
}

// Store is an in-memory, mutex-guarded store.
type Store struct {
	mu            sync.Mutex
	endpoints     map[string]*Endpoint
	events        map[string]*Event
	deliveries    map[string]*Delivery
	eventDelivers map[string][]string  // eventID -> delivery IDs
	pending       map[string]*Delivery // status pending: the set scanned per tick
	inFlight      map[string]int       // endpointID -> in-flight attempts
}

// Job is everything needed to make one delivery attempt without touching the
// store. All fields are copies.
type Job struct {
	DeliveryID    string
	EventID       string
	EndpointID    string
	AttemptNumber int // 1-based, across the whole history
	RetryIndex    int // 0-based position in the current retry cycle; the schedule index used if this attempt fails
	URL           string
	Secret        string
	Body          []byte
}

func New() *Store {
	return &Store{
		endpoints:     make(map[string]*Endpoint),
		events:        make(map[string]*Event),
		deliveries:    make(map[string]*Delivery),
		eventDelivers: make(map[string][]string),
		pending:       make(map[string]*Delivery),
		inFlight:      make(map[string]int),
	}
}

// CreateEndpoint stores ep with a generated ID. URL/secret format checks are
// the caller's job; the store only rejects missing fields.
func (s *Store) CreateEndpoint(ep Endpoint) (Endpoint, error) {
	if ep.URL == "" || ep.Secret == "" || len(ep.EventTypes) == 0 {
		return Endpoint{}, fmt.Errorf("create endpoint: url, secret and event types are required: %w", ErrInvalid)
	}
	id, err := newID("ep_")
	if err != nil {
		return Endpoint{}, fmt.Errorf("create endpoint: %w", err)
	}
	stored := copyEndpoint(ep)
	stored.ID = id

	s.mu.Lock()
	defer s.mu.Unlock()
	s.endpoints[id] = &stored
	return copyEndpoint(stored), nil
}

// CreateEvent stores ev and atomically creates a pending delivery, due at now,
// for every endpoint subscribed to ev.Type. An identical replay of an existing
// event ID returns the existing event and deliveries with created=false;
// different content for the same ID returns ErrConflict.
func (s *Store) CreateEvent(ev Event, now time.Time) (Event, []Delivery, bool, error) {
	if err := validateEvent(ev); err != nil {
		return Event{}, nil, false, fmt.Errorf("create event: %w", err)
	}
	body, err := buildBody(ev)
	if err != nil {
		return Event{}, nil, false, fmt.Errorf("create event %q: %w", ev.ID, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.events[ev.ID]; ok {
		same, err := sameContent(existing, ev)
		if err != nil {
			return Event{}, nil, false, fmt.Errorf("create event %q: %w", ev.ID, err)
		}
		if !same {
			return Event{}, nil, false, fmt.Errorf("create event %q: different content for existing id: %w", ev.ID, ErrConflict)
		}
		return copyEvent(*existing), s.eventDeliveriesLocked(ev.ID), false, nil
	}

	stored := copyEvent(ev)
	stored.ReceivedAt = now
	stored.Body = body

	ids := make([]string, 0)
	newDeliveries := make([]*Delivery, 0)
	for _, ep := range s.endpoints {
		if !slices.Contains(ep.EventTypes, ev.Type) {
			continue
		}
		id, err := newID("dlv_")
		if err != nil {
			return Event{}, nil, false, fmt.Errorf("create event %q: %w", ev.ID, err)
		}
		newDeliveries = append(newDeliveries, &Delivery{
			ID:            id,
			EventID:       ev.ID,
			EndpointID:    ep.ID,
			Status:        StatusPending,
			NextAttemptAt: now,
		})
		ids = append(ids, id)
	}

	// Commit only after every ID was generated, so a failure leaves no partial fan-out.
	s.events[ev.ID] = &stored
	for _, d := range newDeliveries {
		s.deliveries[d.ID] = d
		s.pending[d.ID] = d
	}
	s.eventDelivers[ev.ID] = ids
	return copyEvent(stored), s.eventDeliveriesLocked(ev.ID), true, nil
}

// GetEvent returns the event and its deliveries, sorted by endpoint ID.
func (s *Store) GetEvent(id string) (Event, []Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev, ok := s.events[id]
	if !ok {
		return Event{}, nil, fmt.Errorf("get event %q: %w", id, ErrNotFound)
	}
	return copyEvent(*ev), s.eventDeliveriesLocked(id), nil
}

func (s *Store) GetDelivery(id string) (Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[id]
	if !ok {
		return Delivery{}, fmt.Errorf("get delivery %q: %w", id, ErrNotFound)
	}
	return copyDelivery(*d), nil
}

// ClaimDue moves due pending deliveries (NextAttemptAt <= now) to in_flight,
// oldest NextAttemptAt first, without exceeding perEndpointLimit in-flight
// attempts per endpoint, and returns a Job for each. A limit below 1 is
// treated as 1 so deliveries can never stall.
func (s *Store) ClaimDue(now time.Time, perEndpointLimit int) []Job {
	perEndpointLimit = max(perEndpointLimit, 1)

	s.mu.Lock()
	defer s.mu.Unlock()

	due := make([]*Delivery, 0)
	for _, d := range s.pending {
		if !d.NextAttemptAt.After(now) {
			due = append(due, d)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].NextAttemptAt.Equal(due[j].NextAttemptAt) {
			return due[i].NextAttemptAt.Before(due[j].NextAttemptAt)
		}
		return due[i].ID < due[j].ID
	})

	jobs := make([]Job, 0, len(due))
	for _, d := range due {
		if s.inFlight[d.EndpointID] >= perEndpointLimit {
			continue
		}
		ep := s.endpoints[d.EndpointID]
		d.Status = StatusInFlight
		delete(s.pending, d.ID)
		s.inFlight[d.EndpointID]++
		jobs = append(jobs, Job{
			DeliveryID:    d.ID,
			EventID:       d.EventID,
			EndpointID:    d.EndpointID,
			AttemptNumber: len(d.Attempts) + 1,
			RetryIndex:    len(d.Attempts) - d.cycleStart,
			URL:           ep.URL,
			Secret:        ep.Secret,
			Body:          append([]byte(nil), s.events[d.EventID].Body...),
		})
	}
	return jobs
}

// Complete records attempt a on an in_flight delivery, frees its endpoint slot
// and moves it to next: pending (due at nextAt), succeeded or failed. nextAt is
// ignored for terminal states. It fails without changing anything if the
// delivery is unknown (ErrNotFound), not in_flight (ErrConflict), or the
// arguments are inconsistent (ErrInvalid).
func (s *Store) Complete(deliveryID string, a Attempt, next Status, nextAt time.Time) error {
	switch next {
	case StatusPending:
		if nextAt.IsZero() {
			return fmt.Errorf("complete delivery %q: pending requires next attempt time: %w", deliveryID, ErrInvalid)
		}
	case StatusSucceeded, StatusFailed:
	default:
		return fmt.Errorf("complete delivery %q: invalid next status %q: %w", deliveryID, next, ErrInvalid)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.deliveries[deliveryID]
	if !ok {
		return fmt.Errorf("complete delivery %q: %w", deliveryID, ErrNotFound)
	}
	if d.Status != StatusInFlight {
		return fmt.Errorf("complete delivery %q: status is %q, not in_flight: %w", deliveryID, d.Status, ErrConflict)
	}
	if want := len(d.Attempts) + 1; a.Number != want {
		return fmt.Errorf("complete delivery %q: attempt number %d, want %d: %w", deliveryID, a.Number, want, ErrInvalid)
	}

	d.Attempts = append(d.Attempts, a)
	d.Status = next
	d.NextAttemptAt = time.Time{}
	if next == StatusPending {
		d.NextAttemptAt = nextAt
		s.pending[d.ID] = d
	}
	s.inFlight[d.EndpointID]--
	if s.inFlight[d.EndpointID] <= 0 {
		delete(s.inFlight, d.EndpointID)
	}
	return nil
}

// Redeliver moves a failed delivery back to pending, due at now, with a fresh
// retry budget. Attempt history is kept. It returns ErrNotFound for an unknown
// delivery and ErrConflict if the delivery is not failed.
func (s *Store) Redeliver(id string, now time.Time) (Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.deliveries[id]
	if !ok {
		return Delivery{}, fmt.Errorf("redeliver %q: %w", id, ErrNotFound)
	}
	if d.Status != StatusFailed {
		return Delivery{}, fmt.Errorf("redeliver %q: status is %q, only failed deliveries can be redelivered: %w", id, d.Status, ErrConflict)
	}
	d.Status = StatusPending
	d.NextAttemptAt = now
	d.cycleStart = len(d.Attempts)
	s.pending[d.ID] = d
	return copyDelivery(*d), nil
}

// eventDeliveriesLocked returns copies of an event's deliveries sorted by
// endpoint ID. Caller must hold s.mu.
func (s *Store) eventDeliveriesLocked(eventID string) []Delivery {
	ids := s.eventDelivers[eventID]
	out := make([]Delivery, 0, len(ids))
	for _, id := range ids {
		out = append(out, copyDelivery(*s.deliveries[id]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EndpointID < out[j].EndpointID })
	return out
}

// MaxEventIDLen bounds event IDs, which are sent in the Webhook-Event-Id header.
const MaxEventIDLen = 255

// validateEvent checks required fields. The event ID must be 1..MaxEventIDLen
// visible ASCII characters so it is always a valid HTTP header value; an
// invalid header would make every delivery attempt fail before sending.
func validateEvent(ev Event) error {
	if ev.ID == "" || ev.Type == "" || ev.CreatedAt.IsZero() {
		return fmt.Errorf("id, type and created_at are required: %w", ErrInvalid)
	}
	if len(ev.ID) > MaxEventIDLen {
		return fmt.Errorf("id longer than %d bytes: %w", MaxEventIDLen, ErrInvalid)
	}
	for i := 0; i < len(ev.ID); i++ {
		if c := ev.ID[i]; c < '!' || c > '~' {
			return fmt.Errorf("id must be visible ASCII, got byte %#x at %d: %w", c, i, ErrInvalid)
		}
	}
	if p := bytes.TrimSpace(ev.Payload); len(p) == 0 || string(p) == "null" {
		return fmt.Errorf("payload is required: %w", ErrInvalid)
	}
	return nil
}

// buildBody serializes the envelope sent to merchants. Marshal also rejects
// an invalid payload.
func buildBody(ev Event) ([]byte, error) {
	body, err := json.Marshal(struct {
		EventID   string          `json:"event_id"`
		Type      string          `json:"type"`
		CreatedAt time.Time       `json:"created_at"`
		Payload   json.RawMessage `json:"payload"`
	}{ev.ID, ev.Type, ev.CreatedAt, ev.Payload})
	if err != nil {
		return nil, fmt.Errorf("build body: %v: %w", err, ErrInvalid)
	}
	return body, nil
}

// sameContent compares type, created_at and payload, ignoring JSON whitespace.
func sameContent(existing *Event, ev Event) (bool, error) {
	if existing.Type != ev.Type || !existing.CreatedAt.Equal(ev.CreatedAt) {
		return false, nil
	}
	var a, b bytes.Buffer
	if err := json.Compact(&a, existing.Payload); err != nil {
		return false, fmt.Errorf("compact stored payload: %w", err)
	}
	if err := json.Compact(&b, ev.Payload); err != nil {
		return false, fmt.Errorf("compact payload: %v: %w", err, ErrInvalid)
	}
	return bytes.Equal(a.Bytes(), b.Bytes()), nil
}

func newID(prefix string) (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return prefix + hex.EncodeToString(b), nil
}

func copyEndpoint(ep Endpoint) Endpoint {
	ep.EventTypes = append([]string(nil), ep.EventTypes...)
	return ep
}

func copyEvent(ev Event) Event {
	ev.Payload = append(json.RawMessage(nil), ev.Payload...)
	ev.Body = append([]byte(nil), ev.Body...)
	return ev
}

func copyDelivery(d Delivery) Delivery {
	d.Attempts = append([]Attempt(nil), d.Attempts...)
	return d
}

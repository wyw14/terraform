// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package tracing

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Attr is a structured key/value pair attached to log records and metric
// events. Only strings are used so aggregated records remain comparable
// without retaining references to business-logic values.
type Attr struct {
	Key   string
	Value string
}

// StringAttr constructs an [Attr] from a string value.
func StringAttr(key, value string) Attr {
	return Attr{Key: key, Value: value}
}

func attrsToPairs(attrs []Attr) []Attr {
	if len(attrs) == 0 {
		return nil
	}
	// Copy so later mutation by the caller can't change captured records.
	ret := make([]Attr, len(attrs))
	copy(ret, attrs)
	return ret
}

// LogEvent is one log record attributed to an operation.
type LogEvent struct {
	Time        time.Time
	Level       string
	Message     string
	OperationID OperationID
	Attrs       []Attr
}

// MetricKind identifies what kind of metric an event represents.
type MetricKind int

const (
	// MetricCounter is a monotonically additive value.
	MetricCounter MetricKind = iota
	// MetricTimer is a single duration observation.
	MetricTimer
)

func (k MetricKind) String() string {
	switch k {
	case MetricCounter:
		return "counter"
	case MetricTimer:
		return "timer"
	default:
		return "unknown"
	}
}

// MetricEvent is one metric observation attributed to an operation.
type MetricEvent struct {
	Time        time.Time
	Name        string
	Kind        MetricKind
	Value       float64
	OperationID OperationID
	Attrs       []Attr
}

// registry is the process-wide aggregation surface. It is keyed throughout by
// operation ID, so records from operations running concurrently can never be
// merged together. The mutex guards every map and slice; graph walks in
// particular record from many goroutines at once.
type registryData struct {
	logs      map[OperationID][]LogEvent
	metrics   map[OperationID][]MetricEvent
	openSpans map[OperationID]int
	allOpen   int
}

var (
	registryMu sync.Mutex
	registry   = &registryData{
		logs:      make(map[OperationID][]LogEvent),
		metrics:   make(map[OperationID][]MetricEvent),
		openSpans: make(map[OperationID]int),
	}
)

// RecordLog appends a log record to the operation's aggregate. It is a no-op
// when the context carries no sampled operation, which is what makes turning
// sampling off free and side-effect free.
func RecordLog(ctx context.Context, level, message string, attrs ...Attr) {
	st := stateFromContext(ctx)
	if st == nil || !st.sampled {
		return
	}
	registryMu.Lock()
	registry.logs[st.id] = append(registry.logs[st.id], LogEvent{
		Time:        time.Now(),
		Level:       level,
		Message:     message,
		OperationID: st.id,
		Attrs:       attrsToPairs(attrs),
	})
	registryMu.Unlock()
}

// IncrCounter adds one to the named counter for the context's operation.
func IncrCounter(ctx context.Context, name string, attrs ...Attr) {
	AddCounter(ctx, name, 1, attrs...)
}

// AddCounter adds delta to the named counter for the context's operation.
func AddCounter(ctx context.Context, name string, delta float64, attrs ...Attr) {
	st := stateFromContext(ctx)
	if st == nil || !st.sampled {
		return
	}
	registryMu.Lock()
	registry.metrics[st.id] = append(registry.metrics[st.id], MetricEvent{
		Time:        time.Now(),
		Name:        name,
		Kind:        MetricCounter,
		Value:       delta,
		OperationID: st.id,
		Attrs:       attrsToPairs(attrs),
	})
	registryMu.Unlock()
}

// ObserveDuration records one duration observation for the context's
// operation.
func ObserveDuration(ctx context.Context, name string, d time.Duration, attrs ...Attr) {
	st := stateFromContext(ctx)
	if st == nil || !st.sampled {
		return
	}
	registryMu.Lock()
	registry.metrics[st.id] = append(registry.metrics[st.id], MetricEvent{
		Time:        time.Now(),
		Name:        name,
		Kind:        MetricTimer,
		Value:       float64(d),
		OperationID: st.id,
		Attrs:       attrsToPairs(attrs),
	})
	registryMu.Unlock()
}

// spanBegin/spanEnd track records that are currently open, so tests (and
// future exporters) can assert that a canceled or failed operation leaves no
// unterminated spans behind.
func spanBegin(ctx context.Context) {
	st := stateFromContext(ctx)
	if st == nil || !st.sampled {
		return
	}
	registryMu.Lock()
	registry.openSpans[st.id]++
	registry.allOpen++
	registryMu.Unlock()
}

func spanEnd(ctx context.Context) {
	st := stateFromContext(ctx)
	if st == nil || !st.sampled {
		return
	}
	registryMu.Lock()
	if registry.openSpans[st.id] > 0 {
		registry.openSpans[st.id]--
		if registry.openSpans[st.id] == 0 {
			delete(registry.openSpans, st.id)
		}
	}
	if registry.allOpen > 0 {
		registry.allOpen--
	}
	registryMu.Unlock()
}

// LogEventsForOperation returns all log records captured for the given
// operation, in the order they were recorded. Concurrent operations each get
// their own slice, so records from one operation can never be mixed into
// another's.
func LogEventsForOperation(id OperationID) []LogEvent {
	registryMu.Lock()
	defer registryMu.Unlock()
	return append([]LogEvent(nil), registry.logs[id]...)
}

// MetricEventsForOperation returns all metric events captured for the given
// operation, in the order they were recorded.
func MetricEventsForOperation(id OperationID) []MetricEvent {
	registryMu.Lock()
	defer registryMu.Unlock()
	return append([]MetricEvent(nil), registry.metrics[id]...)
}

// CounterSum returns the summed value of all counter events with the given
// name for the operation.
func CounterSum(id OperationID, name string) float64 {
	var total float64
	for _, ev := range MetricEventsForOperation(id) {
		if ev.Kind == MetricCounter && ev.Name == name {
			total += ev.Value
		}
	}
	return total
}

// OpenSpanCount returns the number of spans for the operation that have begun
// but not yet ended. It must reach zero once the operation and every path it
// started have finished, including when the operation was canceled or failed.
func OpenSpanCount(id OperationID) int {
	registryMu.Lock()
	defer registryMu.Unlock()
	return registry.openSpans[id]
}

// OperationIDs returns the IDs of every operation that has produced any
// captured records so far, sorted for deterministic reporting.
func OperationIDs() []OperationID {
	registryMu.Lock()
	defer registryMu.Unlock()
	seen := make(map[OperationID]struct{})
	for id := range registry.logs {
		seen[id] = struct{}{}
	}
	for id := range registry.metrics {
		seen[id] = struct{}{}
	}
	ret := make([]OperationID, 0, len(seen))
	for id := range seen {
		ret = append(ret, id)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
	return ret
}

// Reset clears all captured records. It is intended for tests that need a
// deterministic aggregate; tests using it must not run in parallel with each
// other.
func Reset() {
	registryMu.Lock()
	registry.logs = make(map[OperationID][]LogEvent)
	registry.metrics = make(map[OperationID][]MetricEvent)
	registry.openSpans = make(map[OperationID]int)
	registry.allOpen = 0
	registryMu.Unlock()
}

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/eventstore"
	"github.com/looplab/eventhorizon/mocks"
	"github.com/looplab/eventhorizon/uuid"
)

var host, port, name, user, password string

func init() {
	if host = os.Getenv("POSTGRES_HOST"); host == "" {
		host = "localhost"
	}
	if port = os.Getenv("POSTGRES_PORT"); port == "" {
		port = "5432"
	}
	if name = os.Getenv("POSTGRES_DATABASE"); name == "" {
		name = "cgrates"
	}
	if user = os.Getenv("POSTGRES_USER"); user == "" {
		user = "postgres"
	}
	if password = os.Getenv("POSTGRES_PASSWORD"); password == "" {
		password = "mysecretpassword"
	}
}

func TestCreateTablesIntegration(t *testing.T) {
	store, err := NewEventStore(host, port, user, password, name)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}
	defer store.Close(context.Background())

	err = store.createTables()
	if err != nil {
		t.Fatal("tables should have been created:", err)
	}
}

func TestEventStoreIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	store, err := NewEventStore(host, port, user, password, name)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}
	defer store.Close(context.Background())

	customNamespaceCtx := eh.NewContextWithNamespace(context.Background(), "ns")

	// Run the actual test suite, both for default and custom namespace.
	eventstore.AcceptanceTest(t, context.Background(), store)
	eventstore.AcceptanceTest(t, customNamespaceCtx, store)
}

func TestWithEventHandlerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	h := &mocks.EventBus{}

	store, err := NewEventStore(host, port, user, password, name, WithEventHandler(h))
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}
	defer store.Close(context.Background())

	ctx := context.Background()

	// The event handler should be called.
	id1 := uuid.New()
	timestamp := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	event1 := eh.NewEventForAggregate(mocks.EventType, &mocks.EventData{Content: "event1"},
		timestamp, mocks.AggregateType, id1, 1)
	err = store.Save(ctx, []eh.Event{event1}, 0)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	expected := []eh.Event{event1}
	// The saved events should be ok.
	events, err := store.Load(ctx, id1)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	// The stored events should be ok.
	if len(events) != len(expected) {
		t.Errorf("incorrect number of loaded events: %d", len(events))
	}
	for i, event := range events {
		if err := eh.CompareEvents(event, expected[i], eh.IgnoreVersion()); err != nil {
			t.Error("the stored event was incorrect:", err)
		}
		if event.Version() != i+1 {
			t.Error("the event version should be correct:", event, event.Version())
		}
	}
	// The handled events should be ok.
	if len(h.Events) != len(expected) {
		t.Errorf("incorrect number of loaded events: %d", len(events))
	}
	for i, event := range h.Events {
		if err := eh.CompareEvents(event, expected[i], eh.IgnoreVersion()); err != nil {
			t.Error("the handeled event was incorrect:", err)
		}
		if event.Version() != i+1 {
			t.Error("the event version should be correct:", event, event.Version())
		}
	}
}

func BenchmarkEventStore(b *testing.B) {
	store, err := NewEventStore(host, port, user, password, name)
	if err != nil {
		b.Fatal("there should be no error:", err)
	}
	if store == nil {
		b.Fatal("there should be a store")
	}
	defer store.Close(context.Background())

	eventstore.Benchmark(b, store)
}

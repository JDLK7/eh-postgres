package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	ErrEventstoreMigration = errors.New("could not create event store tables")
)

// EventStore implements an eventhorizon.EventStore for MongoDB using a single
// collection with one document per aggregate/stream which holds its events
// as values.
type EventStore struct {
	client       *gorm.DB
	dbName       func(ctx context.Context) string
	eventHandler eh.EventHandler
}

// NewEventStore creates a new EventStore...
func NewEventStore(host, port, user, password, database string, options ...Option) (*EventStore, error) {

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", host, user, password, database, port)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("could not connect to DB: %w", err)
	}

	return NewEventStoreWithClient(db, options...)
}

// NewEventStoreWithClient creates a new EventStore with a client.
func NewEventStoreWithClient(client *gorm.DB, options ...Option) (*EventStore, error) {
	if client == nil {
		return nil, fmt.Errorf("missing DB client")
	}

	eventStore := &EventStore{
		client: client,
	}

	for _, option := range options {
		if err := option(eventStore); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	// TODO: maybe shouldn't be its responsability
	err := eventStore.createTables()
	if err != nil {
		eventStore.Close(context.Background())
		return nil, err
	}

	return eventStore, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventStore) error

// WithEventHandler adds an event handler that will be called when saving events.
// An example would be to add an event bus to publish events.
func WithEventHandler(h eh.EventHandler) Option {
	return func(s *EventStore) error {
		s.eventHandler = h
		return nil
	}
}

func (e *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	if len(events) == 0 {
		return eh.EventStoreError{
			Err:       eh.ErrNoEventsToAppend,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	dbEvents := make([]evt, len(events))
	aggregateID := events[0].AggregateID()
	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != aggregateID {
			return eh.EventStoreError{
				Err:       eh.ErrInvalidEvent,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != originalVersion+i+1 {
			return eh.EventStoreError{
				Err:       eh.ErrIncorrectEventVersion,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}

		// Create the event record for the DB.
		e, err := newEvt(ctx, event)
		if err != nil {
			return err
		}
		dbEvents[i] = *e
	}

	// Either insert a new aggregate or append to an existing.
	if originalVersion == 0 {
		aggregate := aggregateRecord{
			AggregateID: aggregateID,
			Version:     len(dbEvents),
			Events:      dbEvents,
		}

		if err := e.client.Create(&aggregate).Error; err != nil {
			return eh.EventStoreError{
				Err:       eh.ErrCouldNotSaveEvents,
				BaseErr:   err,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}
	} else {
		aggregate := aggregateRecord{}

		// Increment aggregate version on insert of new event record, and
		// only insert if version of aggregate is matching (ie not changed
		// since loading the aggregate).
		err := e.client.Where(&aggregateRecord{AggregateID: aggregateID, Version: originalVersion}).
			First(&aggregate).
			Error
		if err != nil {
			return eh.EventStoreError{
				Err:       eh.ErrCouldNotSaveEvents,
				BaseErr:   fmt.Errorf("invalid original version %d", originalVersion),
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}
		err = e.client.Model(&aggregate).
			Updates(aggregateRecord{Version: originalVersion + len(dbEvents)}).
			Association("Events").
			Append(dbEvents)
		if err != nil {
			return eh.EventStoreError{
				Err:       eh.ErrCouldNotSaveEvents,
				BaseErr:   err,
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}
	}

	// Let the optional event handler handle the events.
	if e.eventHandler != nil {
		for _, ev := range events {
			if err := e.eventHandler.HandleEvent(ctx, ev); err != nil {
				return eh.CouldNotHandleEventError{
					Err:       err,
					Event:     ev,
					Namespace: eh.NamespaceFromContext(ctx),
				}
			}
		}
	}

	return nil
}

func (e *EventStore) Load(ctx context.Context, uuid uuid.UUID) ([]eh.Event, error) {
	var aggregate aggregateRecord
	err := e.client.Preload("Events", func(db *gorm.DB) *gorm.DB {
		return db.Order("events.version ASC")
	}).First(&aggregate, uuid).Error
	if err == gorm.ErrRecordNotFound {
		return []eh.Event{}, nil
	} else if err != nil {
		return nil, eh.EventStoreError{
			Err:       fmt.Errorf("could not find event: %w", err),
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	events := make([]eh.Event, len(aggregate.Events))
	for i, ev := range aggregate.Events {
		// Create an event of the correct type and decode from raw JSON.
		if len(ev.Data) > 0 {
			var err error
			if ev.data, err = eh.CreateEventData(ev.EventType); err != nil {
				return nil, eh.EventStoreError{
					Err:       fmt.Errorf("could not create event data: %w", err),
					Namespace: eh.NamespaceFromContext(ctx),
				}
			}
			if err := json.Unmarshal(ev.Data, ev.data); err != nil {
				return nil, eh.EventStoreError{
					Err:       fmt.Errorf("could not unmarshal event data: %w", err),
					Namespace: eh.NamespaceFromContext(ctx),
				}
			}
			ev.Data = nil
		}
		if len(ev.Metadata) > 0 {
			if err := json.Unmarshal(ev.Metadata, &ev.metadata); err != nil {
				return nil, eh.EventStoreError{
					Err:       fmt.Errorf("could not unmarshal event metadata: %w", err),
					Namespace: eh.NamespaceFromContext(ctx),
				}
			}
			ev.Metadata = nil
		}

		event := eh.NewEvent(
			ev.EventType,
			ev.data,
			ev.Timestamp,
			eh.ForAggregate(
				ev.AggregateType,
				ev.AggregateID,
				ev.Version,
			),
			eh.WithMetadata(ev.metadata),
		)
		events[i] = event
	}

	return events, nil
}

func (e *EventStore) createTables() error {
	if err := e.client.AutoMigrate(&aggregateRecord{}, &evt{}); err != nil {
		return eh.EventStoreError{
			Err:       ErrEventstoreMigration,
			BaseErr:   err,
		}
	}
	return nil
}

// Close closes the database client.
// TODO: maybe should not be closed.
func (e *EventStore) Close(ctx context.Context) error {
	db, err := e.client.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

// aggregateRecord is the Database representation of an aggregate.
type aggregateRecord struct {
	AggregateID uuid.UUID `gorm:"primaryKey"`
	Version     int
	Events      []evt `gorm:"foreignKey:AggregateID;references:AggregateID"`
}

func (aggregateRecord) TableName() string {
	return "aggregates"
}

// evt is the internal event record for the MongoDB event store used
// to save and load events from the DB.
type evt struct {
	AggregateID   uuid.UUID `gorm:"primaryKey"`
	Version       int       `gorm:"primaryKey"`
	AggregateType eh.AggregateType
	EventType     eh.EventType
	Data          []byte                 `gorm:"type:jsonb"`
	data          eh.EventData           `gorm:"-"`
	Metadata      []byte                 `gorm:"type:jsonb"`
	metadata      map[string]interface{} `gorm:"-"`
	Timestamp     time.Time
}

func (evt) TableName() string {
	return "events"
}

// newEvt returns a new evt for an event.
func newEvt(ctx context.Context, event eh.Event) (*evt, error) {
	e := &evt{
		EventType:     event.EventType(),
		Timestamp:     event.Timestamp(),
		AggregateType: event.AggregateType(),
		AggregateID:   event.AggregateID(),
		Version:       event.Version(),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error
		e.Data, err = json.Marshal(event.Data())
		if err != nil {
			return nil, eh.EventStoreError{
				Err:       fmt.Errorf("could not marshal event data: %w", err),
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}
	}

	// Marshal event metadata if there is any.
	if event.Metadata() != nil {
		var err error
		e.Metadata, err = json.Marshal(event.Metadata())
		if err != nil {
			return nil, eh.EventStoreError{
				Err:       fmt.Errorf("could not marshal event metadata: %w", err),
				Namespace: eh.NamespaceFromContext(ctx),
			}
		}
	}

	return e, nil
}

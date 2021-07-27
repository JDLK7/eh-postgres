package postgres

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm/clause"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	// ErrCouldNotDialDB is when the database could not be dialed.
	ErrCouldNotDialDB = errors.New("could not dial database")
	// ErrNoDBClient is when no database client is set.
	ErrNoDBClient = errors.New("no database client")
	// ErrCouldNotClearDB is when the database could not be cleared.
	ErrCouldNotClearDB = errors.New("could not clear database")
	// ErrModelNotSet is when an model factory is not set on the Repo.
	ErrModelNotSet = errors.New("model not set")
	// ErrInvalidQuery is when a query was not returned from the callback to FindCustom.
	ErrInvalidQuery = errors.New("invalid query")
)

// Repo implements a PostgreSQL repo for entities.
type Repo struct {
	client *gorm.DB
	factoryFn func() eh.Entity
}

func NewRepo(host, port, user, password, database string) (*Repo, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC", host, user, password, database, port)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("could not connect to DB: %w", err)
	}

	return NewRepoWithClient(db)
}

func NewRepoWithClient(client *gorm.DB) (*Repo, error) {
	if client == nil {
		return nil, ErrNoDBClient
	}

	return &Repo{client: client}, nil
}

func (r *Repo) MigrateTables(entity interface{}) error {
	return r.client.AutoMigrate(entity)
}

// SetEntityFactory sets a factory function that creates concrete entity types.
func (r *Repo) SetEntityFactory(f func() eh.Entity) {
	r.factoryFn = f
}

func (r *Repo) Parent() eh.ReadRepo {
	return nil
}

func (r *Repo) Find(ctx context.Context, id uuid.UUID) (eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	entity := r.factoryFn()
	if err := r.client.WithContext(ctx).Preload(clause.Associations).First(entity, "id = ?", id.String()).Error; err == gorm.ErrRecordNotFound {
		return nil, eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	} else if err != nil {
		return nil, eh.RepoError{
			Err:       eh.ErrCouldNotLoadEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return entity, nil
}

func (r *Repo) FindAll(ctx context.Context) ([]eh.Entity, error) {
	return []eh.Entity{}, nil
	//if r.factoryFn == nil {
	//	return nil, eh.RepoError{
	//		Err:       ErrModelNotSet,
	//		Namespace: eh.NamespaceFromContext(ctx),
	//	}
	//}
	//
	//entities := r.sliceFactoryFn()
	//
	//
	//err := r.client.Find(&entities).Error
	//if err != nil {
	//	return nil, eh.RepoError{
	//		Err:       eh.ErrCouldNotLoadEntity,
	//		BaseErr:   err,
	//		Namespace: eh.NamespaceFromContext(ctx),
	//	}
	//}
	//
	//entities := make([]eh.Entity)
	//for _, record := range result {
	//
	//}
}

func (r *Repo) Save(ctx context.Context, entity eh.Entity) error {
	if entity.EntityID() == uuid.Nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   eh.ErrMissingEntityID,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	if err := r.client.WithContext(ctx).Save(entity).Error; err != nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil
}

func (r *Repo) Remove(ctx context.Context, id uuid.UUID) error {
	if r.factoryFn == nil {
		return eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	entity := r.factoryFn()
	result := r.client.WithContext(ctx).Delete(entity, "id = ?", id.String())
	if err := result.Error; err != nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotRemoveEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	} else if result.RowsAffected == 0 {
		return eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil
}

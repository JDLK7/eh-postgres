package postgres

import (
	"context"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/mocks"
	"github.com/looplab/eventhorizon/repo"
	"os"
	"testing"
)

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

func TestReadRepoIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	r, err := NewRepo(host, port, user, password, name)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if r == nil {
		t.Error("there should be a repository")
	}
	//defer r.Close(context.Background())

	r.SetEntityFactory(func() eh.Entity {
		return &mocks.Model{}
	})

	if r.MigrateTables(&mocks.Model{}) != nil {
		t.Error("could not create model table")
	}

	if r.Parent() != nil {
		t.Error("the parent repo should be nil")
	}

	customNamespaceCtx := eh.NewContextWithNamespace(context.Background(), "ns")

	repo.AcceptanceTest(t, context.Background(), r)
	repo.AcceptanceTest(t, customNamespaceCtx, r)
}


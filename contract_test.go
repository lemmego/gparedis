package gparedis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/lemmego/gpa"
)

func newContractRepository(t *testing.T) (*Repository[TestValue], *redis.Client, func()) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	provider := &Provider{client: client, repositoryKeyPrefix: "contract:"}
	return NewRepository[TestValue](provider, client, "contract:"), client, func() {
		_ = client.Close()
		server.Close()
	}
}

func TestRepositoryContractTTLAndMissingKeys(t *testing.T) {
	repo, _, cleanup := newContractRepository(t)
	defer cleanup()

	ctx := context.Background()
	if err := repo.SetWithTTL(ctx, "ttl", &TestValue{ID: "ttl", Name: "value"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	ttl, err := repo.TTL(ctx, "ttl")
	if err != nil || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("unexpected TTL %v, error %v", ttl, err)
	}

	missingTTL, err := repo.TTL(ctx, "missing")
	if err != nil || missingTTL != -2*time.Nanosecond {
		t.Fatalf("expected Redis missing-key TTL, got %v, error %v", missingTTL, err)
	}
	_, err = repo.Get(ctx, "missing")
	if !gpa.IsErrorType(err, gpa.ErrorTypeNotFound) {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

func TestRepositoryContractSerialization(t *testing.T) {
	repo, client, cleanup := newContractRepository(t)
	defer cleanup()

	if err := client.Set(context.Background(), "contract:bad", "not-json", 0).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := repo.Get(context.Background(), "bad")
	if !gpa.IsErrorType(err, gpa.ErrorTypeSerialization) {
		t.Fatalf("expected serialization error, got %v", err)
	}
}

func TestRepositoryContractCancellation(t *testing.T) {
	repo, _, cleanup := newContractRepository(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := repo.Get(ctx, "cancelled")
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestRepositoryKeyPrefixConfiguration(t *testing.T) {
	if _, err := repositoryKeyPrefix(gpa.Config{Options: map[string]interface{}{
		"redis": map[string]interface{}{"key_prefix": "   "},
	}}); err == nil {
		t.Fatal("expected whitespace key prefix to be rejected")
	}
	prefix, err := repositoryKeyPrefix(gpa.Config{})
	if err != nil || prefix != DefaultRepositoryKeyPrefix {
		t.Fatalf("expected default prefix %q, got %q, error %v", DefaultRepositoryKeyPrefix, prefix, err)
	}
}

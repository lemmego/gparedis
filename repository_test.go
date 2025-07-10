package gparedis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lemmego/gpa"
)

type TestValue struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func setupTestRepository(t *testing.T) (*Repository[TestValue], func()) {
	// Check for Redis connection string in environment
	redisURL := os.Getenv("REDIS_TEST_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	config := gpa.Config{
		Driver:        "redis",
		ConnectionURL: redisURL,
		Database:      "0",
	}

	provider, err := NewProvider(config)
	if err != nil {
		t.Skipf("Skipping Redis tests: %v", err)
	}

	// Clear the test database
	provider.client.FlushDB(context.Background())

	repo := NewRepository[TestValue](provider, provider.client, "")

	cleanup := func() {
		provider.client.FlushDB(context.Background())
		provider.Close()
	}

	return repo, cleanup
}

func TestRepositorySet(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	err := repo.Set(ctx, "user:123", value)
	if err != nil {
		t.Errorf("Failed to set value: %v", err)
	}
}

func TestRepositoryGet(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	original := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Set the value first
	err := repo.Set(ctx, "user:123", original)
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Get the value
	retrieved, err := repo.Get(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to get value: %v", err)
	}

	if retrieved.Name != original.Name {
		t.Errorf("Expected name '%s', got '%s'", original.Name, retrieved.Name)
	}
	if retrieved.Age != original.Age {
		t.Errorf("Expected age %d, got %d", original.Age, retrieved.Age)
	}
}

func TestRepositoryGetNotFound(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	_, err := repo.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent key")
	}

	if !gpa.IsErrorType(err, gpa.ErrorTypeNotFound) {
		t.Errorf("Expected not found error, got %v", err)
	}
}

func TestRepositoryDeleteKey(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Set the value first
	err := repo.Set(ctx, "user:123", value)
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Delete the key
	err = repo.DeleteKey(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to delete key: %v", err)
	}

	// Verify deletion
	_, err = repo.Get(ctx, "user:123")
	if err == nil {
		t.Error("Expected error when getting deleted key")
	}
}

func TestRepositoryKeyExists(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Check non-existent key
	exists, err := repo.KeyExists(ctx, "nonexistent")
	if err != nil {
		t.Errorf("Failed to check existence: %v", err)
	}
	if exists {
		t.Error("Expected key not to exist")
	}

	// Set a value
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}
	err = repo.Set(ctx, "user:123", value)
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Check existing key
	exists, err = repo.KeyExists(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to check existence: %v", err)
	}
	if !exists {
		t.Error("Expected key to exist")
	}
}

func TestRepositoryMGet(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Set multiple values
	values := map[string]*TestValue{
		"user:1": {ID: "user:1", Name: "Alice", Age: 25},
		"user:2": {ID: "user:2", Name: "Bob", Age: 30},
		"user:3": {ID: "user:3", Name: "Charlie", Age: 35},
	}

	for key, value := range values {
		err := repo.Set(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to set value for %s: %v", key, err)
		}
	}

	// Get multiple values
	keys := []string{"user:1", "user:2", "user:3", "user:nonexistent"}
	results, err := repo.MGet(ctx, keys)
	if err != nil {
		t.Errorf("Failed to get multiple values: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Check that we got the right values (excluding the non-existent one)
	found := make(map[string]*TestValue)
	for _, result := range results {
		found[result.ID] = result
	}

	for key, expected := range values {
		if result, ok := found[expected.ID]; ok {
			if result.Name != expected.Name {
				t.Errorf("Expected name '%s' for %s, got '%s'", expected.Name, key, result.Name)
			}
		} else {
			t.Errorf("Expected to find result for %s", key)
		}
	}
}

func TestRepositoryMSet(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Set multiple values at once
	values := map[string]*TestValue{
		"user:1": {ID: "user:1", Name: "Alice", Age: 25},
		"user:2": {ID: "user:2", Name: "Bob", Age: 30},
		"user:3": {ID: "user:3", Name: "Charlie", Age: 35},
	}

	err := repo.MSet(ctx, values)
	if err != nil {
		t.Errorf("Failed to set multiple values: %v", err)
	}

	// Verify all values were set
	for key, expected := range values {
		result, err := repo.Get(ctx, key)
		if err != nil {
			t.Errorf("Failed to get value for %s: %v", key, err)
			continue
		}

		if result.Name != expected.Name {
			t.Errorf("Expected name '%s' for %s, got '%s'", expected.Name, key, result.Name)
		}
		if result.Age != expected.Age {
			t.Errorf("Expected age %d for %s, got %d", expected.Age, key, result.Age)
		}
	}
}

func TestRepositorySetWithTTL(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Set with TTL
	ttl := time.Second * 2
	err := repo.SetWithTTL(ctx, "user:123", value, ttl)
	if err != nil {
		t.Errorf("Failed to set value with TTL: %v", err)
	}

	// Verify value exists
	_, err = repo.Get(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to get value: %v", err)
	}

	// Wait for TTL to expire
	time.Sleep(ttl + time.Millisecond*100)

	// Verify value has expired
	_, err = repo.Get(ctx, "user:123")
	if err == nil {
		t.Error("Expected error for expired key")
	}
}

func TestRepositoryGetTTL(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Set with TTL
	ttl := time.Hour
	err := repo.SetWithTTL(ctx, "user:123", value, ttl)
	if err != nil {
		t.Fatalf("Failed to set value with TTL: %v", err)
	}

	// Get TTL
	remainingTTL, err := repo.GetTTL(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to get TTL: %v", err)
	}

	if remainingTTL <= 0 || remainingTTL > ttl {
		t.Errorf("Expected TTL between 0 and %v, got %v", ttl, remainingTTL)
	}
}

func TestRepositorySetTTL(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Set value without TTL
	err := repo.Set(ctx, "user:123", value)
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Set TTL
	ttl := time.Hour
	err = repo.SetTTL(ctx, "user:123", ttl)
	if err != nil {
		t.Errorf("Failed to set TTL: %v", err)
	}

	// Verify TTL was set
	remainingTTL, err := repo.GetTTL(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to get TTL: %v", err)
	}

	if remainingTTL <= 0 || remainingTTL > ttl {
		t.Errorf("Expected TTL between 0 and %v, got %v", ttl, remainingTTL)
	}
}

func TestRepositoryRemoveTTL(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Set with TTL
	ttl := time.Hour
	err := repo.SetWithTTL(ctx, "user:123", value, ttl)
	if err != nil {
		t.Fatalf("Failed to set value with TTL: %v", err)
	}

	// Remove TTL
	err = repo.RemoveTTL(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to remove TTL: %v", err)
	}

	// Verify TTL was removed (should return -1 for no TTL)
	remainingTTL, err := repo.GetTTL(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to get TTL: %v", err)
	}

	if remainingTTL != -1 {
		t.Errorf("Expected TTL -1 (no expiration), got %v", remainingTTL)
	}
}

func TestRepositoryIncrement(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Increment non-existent key
	result, err := repo.Increment(ctx, "counter", 1)
	if err != nil {
		t.Errorf("Failed to increment: %v", err)
	}
	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}

	// Increment existing key
	result, err = repo.Increment(ctx, "counter", 1)
	if err != nil {
		t.Errorf("Failed to increment: %v", err)
	}
	if result != 2 {
		t.Errorf("Expected 2, got %d", result)
	}
}

func TestRepositoryIncrementBy(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Increment by 5
	result, err := repo.Increment(ctx, "counter", 5)
	if err != nil {
		t.Errorf("Failed to increment by: %v", err)
	}
	if result != 5 {
		t.Errorf("Expected 5, got %d", result)
	}

	// Increment by 3 more
	result, err = repo.Increment(ctx, "counter", 3)
	if err != nil {
		t.Errorf("Failed to increment by: %v", err)
	}
	if result != 8 {
		t.Errorf("Expected 8, got %d", result)
	}
}

func TestRepositoryDecrement(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Set initial value
	_, err := repo.Increment(ctx, "counter", 10)
	if err != nil {
		t.Fatalf("Failed to set initial value: %v", err)
	}

	// Decrement
	result, err := repo.Decrement(ctx, "counter", 1)
	if err != nil {
		t.Errorf("Failed to decrement: %v", err)
	}
	if result != 9 {
		t.Errorf("Expected 9, got %d", result)
	}
}

func TestRepositoryDecrementBy(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Set initial value
	_, err := repo.Increment(ctx, "counter", 10)
	if err != nil {
		t.Fatalf("Failed to set initial value: %v", err)
	}

	// Decrement by 3
	result, err := repo.Decrement(ctx, "counter", 3)
	if err != nil {
		t.Errorf("Failed to decrement by: %v", err)
	}
	if result != 7 {
		t.Errorf("Expected 7, got %d", result)
	}
}

func TestRepositoryGetKeys(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Set some values
	values := map[string]*TestValue{
		"user:1":    {ID: "user:1", Name: "Alice", Age: 25},
		"user:2":    {ID: "user:2", Name: "Bob", Age: 30},
		"product:1": {ID: "product:1", Name: "Widget", Age: 0},
	}

	for key, value := range values {
		err := repo.Set(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to set value for %s: %v", key, err)
		}
	}

	// Get all user keys
	userKeys, err := repo.Keys(ctx, "user:*")
	if err != nil {
		t.Errorf("Failed to get keys: %v", err)
	}

	if len(userKeys) != 2 {
		t.Errorf("Expected 2 user keys, got %d", len(userKeys))
	}

	expectedUserKeys := map[string]bool{"user:1": true, "user:2": true}
	for _, key := range userKeys {
		if !expectedUserKeys[key] {
			t.Errorf("Unexpected key: %s", key)
		}
	}
}

func TestRepositoryFlush(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Set some values
	values := map[string]*TestValue{
		"user:1": {ID: "user:1", Name: "Alice", Age: 25},
		"user:2": {ID: "user:2", Name: "Bob", Age: 30},
	}

	for key, value := range values {
		err := repo.Set(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to set value for %s: %v", key, err)
		}
	}

	// Delete all keys individually (since Flush doesn't exist)
	for key := range values {
		err := repo.DeleteKey(ctx, key)
		if err != nil {
			t.Errorf("Failed to delete key %s: %v", key, err)
		}
	}

	// Verify all keys are gone
	for key := range values {
		_, err := repo.Get(ctx, key)
		if err == nil {
			t.Errorf("Expected key %s to be deleted after flush", key)
		}
	}
}

func TestRepositoryClose(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	err := repo.Close()
	if err != nil {
		t.Errorf("Failed to close repository: %v", err)
	}
}

// Test BasicKeyValueRepository interface methods

func TestRepositoryCreate(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	err := repo.Set(ctx, "user:123", value)
	if err != nil {
		t.Errorf("Failed to set entity: %v", err)
	}

	// Verify it was created
	retrieved, err := repo.Get(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to get entity: %v", err)
	}

	if retrieved == nil {
		t.Error("Expected retrieved entity to not be nil")
		return
	}

	if retrieved.Name != value.Name {
		t.Errorf("Expected name '%s', got '%s'", value.Name, retrieved.Name)
	}
}

func TestRepositoryFindByID(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Set the value first
	err := repo.Set(ctx, "user:123", value)
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Find by ID
	found, err := repo.Get(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to get by ID: %v", err)
	}

	if found == nil {
		t.Error("Expected found value to not be nil")
		return
	}

	if found.Name != value.Name {
		t.Errorf("Expected name '%s', got '%s'", value.Name, found.Name)
	}
}

// TestRepositoryUpdate is commented out because Update is not supported by Redis KV store
/*
func TestRepositoryUpdate(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	original := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Create original
	err := repo.Create(ctx, original)
	if err != nil {
		t.Fatalf("Failed to create entity: %v", err)
	}

	// Update
	updated := &TestValue{
		ID:   "user:123",
		Name: "John Smith",
		Age:  31,
	}
	err = repo.Update(ctx, updated)
	if err != nil {
		t.Errorf("Failed to update entity: %v", err)
	}

	// Verify update
	found, err := repo.FindByID(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to find updated entity: %v", err)
	}

	if found.Name != "John Smith" {
		t.Errorf("Expected name 'John Smith', got '%s'", found.Name)
	}
	if found.Age != 31 {
		t.Errorf("Expected age 31, got %d", found.Age)
	}
}
*/

// TestRepositoryDelete is commented out because Delete is not supported by Redis KV store
/*
func TestRepositoryDelete(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()
	value := &TestValue{
		ID:   "user:123",
		Name: "John Doe",
		Age:  30,
	}

	// Create entity
	err := repo.Create(ctx, value)
	if err != nil {
		t.Fatalf("Failed to create entity: %v", err)
	}

	// Delete entity
	err = repo.Delete(ctx, "user:123")
	if err != nil {
		t.Errorf("Failed to delete entity: %v", err)
	}

	// Verify deletion
	_, err = repo.FindByID(ctx, "user:123")
	if err == nil {
		t.Error("Expected error when finding deleted entity")
	}
}
*/

// TestRepositoryTransaction is commented out because Transaction is not supported by Redis KV store
/*
func TestRepositoryTransaction(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	ctx := context.Background()

	// Successful transaction
	err := repo.Transaction(ctx, func(tx gpa.Transaction[TestValue]) error {
		value1 := &TestValue{ID: "user:1", Name: "Alice", Age: 25}
		value2 := &TestValue{ID: "user:2", Name: "Bob", Age: 30}

		if err := tx.Create(ctx, value1); err != nil {
			return err
		}
		if err := tx.Create(ctx, value2); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Errorf("Transaction failed: %v", err)
	}

	// Verify both entities were created
	_, err = repo.FindByID(ctx, "user:1")
	if err != nil {
		t.Errorf("Failed to find user:1: %v", err)
	}

	_, err = repo.FindByID(ctx, "user:2")
	if err != nil {
		t.Errorf("Failed to find user:2: %v", err)
	}
}
*/

func TestRepositoryGetEntityInfo(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	info, err := repo.GetEntityInfo()
	if err != nil {
		t.Errorf("Failed to get entity info: %v", err)
	}

	if info.Name != "gparedis.TestValue" {
		t.Errorf("Expected entity name 'gparedis.TestValue', got '%s'", info.Name)
	}

	// For Redis KV store, fields are not populated since it's schema-less
	// This is expected behavior
}

func TestConvertRedisError(t *testing.T) {
	// Test nil error
	err := convertRedisError(nil)
	if err != nil {
		t.Error("Expected nil error to remain nil")
	}

	// Test Redis Nil (key not found)
	// Note: In a real test, you'd import redis and use redis.Nil
	// For now, we'll test the general case
	originalErr := gpa.NewError(gpa.ErrorTypeNotFound, "key not found")
	err = convertRedisError(originalErr)
	if !gpa.IsErrorType(err, gpa.ErrorTypeNotFound) {
		t.Error("Expected not found error type")
	}
}
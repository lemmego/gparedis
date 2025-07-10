package gparedis

import (
	"context"
	"testing"
	"time"

	"github.com/lemmego/gpa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TypeSafeTestUser represents a test entity for Redis type-safe testing
type TypeSafeTestUser struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Email   string    `json:"email"`
	Age     int       `json:"age"`
	Active  bool      `json:"active"`
	Created time.Time `json:"created"`
}

// TypeSafeTestSession represents a session entity for Redis type-safe testing
type TypeSafeTestSession struct {
	ID        string            `json:"id"`
	UserID    string            `json:"user_id"`
	Data      map[string]string `json:"data"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// TestGenericRedisRepository demonstrates type-safe Redis operations
func TestGenericRedisRepository(t *testing.T) {
	// Skip if no Redis available
	if testing.Short() {
		t.Skip("Skipping Redis integration test in short mode")
	}

	// Create Redis client (you would configure this based on your setup)
	config := gpa.Config{
		Host:     "localhost",
		Port:     6379,
		Database: "0",
	}

	provider, err := createProvider(config)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer provider.Close()

	redisProvider := provider
	ctx := context.Background()

	t.Run("BasicKeyValueRepositoryG operations", func(t *testing.T) {
		// Create a type-safe repository for users
		userRepo := NewRepository[TypeSafeTestUser](redisProvider, redisProvider.client, "user:")

		// Test data
		user := &TypeSafeTestUser{
			ID:      "123",
			Name:    "John Doe",
			Email:   "john@example.com",
			Age:     30,
			Active:  true,
			Created: time.Now(),
		}

		// Set with compile-time type safety
		err := userRepo.Set(ctx, user.ID, user)
		require.NoError(t, err)

		// Get with compile-time type safety - returns *TypeSafeTestUser directly
		retrievedUser, err := userRepo.Get(ctx, user.ID)
		require.NoError(t, err)
		assert.Equal(t, user.Name, retrievedUser.Name)
		assert.Equal(t, user.Email, retrievedUser.Email)
		assert.Equal(t, user.Age, retrievedUser.Age)
		assert.Equal(t, user.Active, retrievedUser.Active)

		// Check existence
		exists, err := userRepo.KeyExists(ctx, user.ID)
		require.NoError(t, err)
		assert.True(t, exists)

		// Delete
		err = userRepo.DeleteKey(ctx, user.ID)
		require.NoError(t, err)

		// Verify deletion
		exists, err = userRepo.KeyExists(ctx, user.ID)
		require.NoError(t, err)
		assert.False(t, exists)

		// Get non-existent key should return error
		_, err = userRepo.Get(ctx, "nonexistent")
		assert.Error(t, err)
		gpaErr, ok := err.(gpa.GPAError)
		assert.True(t, ok)
		assert.Equal(t, gpa.ErrorTypeNotFound, gpaErr.Type)
	})

	t.Run("BatchKeyValueRepositoryG operations", func(t *testing.T) {
		userRepo := NewRepository[TypeSafeTestUser](redisProvider, redisProvider.client, "batch_user:")

		// Test data
		users := map[string]*TypeSafeTestUser{
			"1": {ID: "1", Name: "Alice", Email: "alice@example.com", Age: 25, Active: true},
			"2": {ID: "2", Name: "Bob", Email: "bob@example.com", Age: 30, Active: false},
			"3": {ID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 35, Active: true},
		}

		// Batch set with type safety
		err := userRepo.MSet(ctx, users)
		require.NoError(t, err)

		// Batch get with type safety - returns []*TypeSafeTestUser
		retrievedUsers, err := userRepo.MGet(ctx, []string{"1", "2", "3"})
		require.NoError(t, err)
		assert.Len(t, retrievedUsers, 3)

		// Verify data
		userMap := make(map[string]*TypeSafeTestUser)
		for _, user := range retrievedUsers {
			userMap[user.ID] = user
		}

		assert.Equal(t, "Alice", userMap["1"].Name)
		assert.Equal(t, "Bob", userMap["2"].Name)
		assert.Equal(t, "Charlie", userMap["3"].Name)

		// Batch delete
		deleted, err := userRepo.MDelete(ctx, []string{"1", "2", "3"})
		require.NoError(t, err)
		assert.Equal(t, int64(3), deleted)

		// Verify deletion
		retrievedUsers, err = userRepo.MGet(ctx, []string{"1", "2", "3"})
		require.NoError(t, err)
		assert.Len(t, retrievedUsers, 0)
	})

	t.Run("TTLKeyValueRepositoryG operations", func(t *testing.T) {
		sessionRepo := NewRepository[TypeSafeTestSession](redisProvider, redisProvider.client, "session:")

		session := &TypeSafeTestSession{
			ID:        "session123",
			UserID:    "user456",
			Data:      map[string]string{"theme": "dark", "language": "en"},
			ExpiresAt: time.Now().Add(1 * time.Hour),
		}

		// Set with TTL
		err := sessionRepo.SetWithTTL(ctx, session.ID, session, 5*time.Second)
		require.NoError(t, err)

		// Get before expiration
		retrievedSession, err := sessionRepo.Get(ctx, session.ID)
		require.NoError(t, err)
		assert.Equal(t, session.UserID, retrievedSession.UserID)
		assert.Equal(t, session.Data["theme"], retrievedSession.Data["theme"])

		// Check TTL
		ttl, err := sessionRepo.TTL(ctx, session.ID)
		require.NoError(t, err)
		assert.Greater(t, ttl, time.Duration(0))
		assert.LessOrEqual(t, ttl, 5*time.Second)

		// Update TTL
		err = sessionRepo.SetTTL(ctx, session.ID, 10*time.Second)
		require.NoError(t, err)

		// Verify updated TTL
		newTTL, err := sessionRepo.TTL(ctx, session.ID)
		require.NoError(t, err)
		assert.Greater(t, newTTL, 5*time.Second)

		// Clean up
		err = sessionRepo.DeleteKey(ctx, session.ID)
		require.NoError(t, err)
	})

	t.Run("Numeric operations", func(t *testing.T) {
		counterRepo := NewRepository[TypeSafeTestUser](redisProvider, redisProvider.client, "counter:")

		// Test increment operations
		count, err := counterRepo.Increment(ctx, "page_views", 1)
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)

		count, err = counterRepo.Increment(ctx, "page_views", 5)
		require.NoError(t, err)
		assert.Equal(t, int64(6), count)

		// Test decrement operations
		count, err = counterRepo.Decrement(ctx, "page_views", 2)
		require.NoError(t, err)
		assert.Equal(t, int64(4), count)

		// Clean up
		err = counterRepo.DeleteKey(ctx, "page_views")
		require.NoError(t, err)
	})

	t.Run("Pattern operations", func(t *testing.T) {
		patternRepo := NewRepository[TypeSafeTestUser](redisProvider, redisProvider.client, "pattern:")

		// Set up test data with unique keys for this test
		testUsers := map[string]*TypeSafeTestUser{
			"pattern_user:1":  {ID: "1", Name: "Alice"},
			"pattern_user:2":  {ID: "2", Name: "Bob"},
			"pattern_admin:1": {ID: "3", Name: "Charlie"},
		}

		err := patternRepo.MSet(ctx, testUsers)
		require.NoError(t, err)

		// Test Keys pattern matching
		userKeys, err := patternRepo.Keys(ctx, "pattern_user:*")
		require.NoError(t, err)
		assert.Len(t, userKeys, 2)
		assert.Contains(t, userKeys, "pattern_user:1")
		assert.Contains(t, userKeys, "pattern_user:2")

		// Test Scan pattern matching - just check it returns results
		allKeys, _, err := patternRepo.Scan(ctx, 0, "pattern_*", 10)
		require.NoError(t, err)
		// Redis scan may return results in batches, so just verify we get some results
		assert.GreaterOrEqual(t, len(allKeys), 1) // Should have at least some keys

		// Clean up
		_, err = patternRepo.MDelete(ctx, []string{"pattern_user:1", "pattern_user:2", "pattern_admin:1"})
		require.NoError(t, err)
	})

	t.Run("AdvancedKeyValueRepositoryG interface", func(t *testing.T) {
		// Test that we can use the repository as the advanced interface
		var advRepo gpa.AdvancedKeyValueRepository[TypeSafeTestUser]
		advRepo = NewAdvancedKVRepository[TypeSafeTestUser](redisProvider, redisProvider.client, "advanced:")

		user := &TypeSafeTestUser{
			ID:   "advanced1",
			Name: "Advanced User",
			Age:  42,
		}

		// All operations work through the interface
		err := advRepo.Set(ctx, user.ID, user)
		require.NoError(t, err)

		retrievedUser, err := advRepo.Get(ctx, user.ID)
		require.NoError(t, err)
		assert.Equal(t, user.Name, retrievedUser.Name)

		// Clean up
		err = advRepo.DeleteKey(ctx, user.ID)
		require.NoError(t, err)
	})
}

// TestTypeSafeRedisOperations demonstrates type-safe Redis operations
func TestTypeSafeRedisOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Redis integration test in short mode")
	}

	config := gpa.Config{
		Host:     "localhost",
		Port:     6379,
		Database: "0",
	}

	provider, err := createProvider(config)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer provider.Close()

	redisProvider := provider
	ctx := context.Background()

	// Create type-safe repository
	typeSafeRepo := NewRepository[TypeSafeTestUser](redisProvider, redisProvider.client, "test:")

	user := &TypeSafeTestUser{
		ID:   "test_user",
		Name: "Test User",
		Age:  30,
	}

	t.Run("Type-safe operations", func(t *testing.T) {
		// Type-safe approach - direct and efficient
		err := typeSafeRepo.Set(ctx, user.ID, user)
		require.NoError(t, err)

		// Get with compile-time type safety
		retrievedUser, err := typeSafeRepo.Get(ctx, user.ID)
		require.NoError(t, err)
		assert.Equal(t, user.Name, retrievedUser.Name)

		// Clean up
		err = typeSafeRepo.DeleteKey(ctx, user.ID)
		require.NoError(t, err)
	})

	// Note: The type-safe approach provides:
	// 1. Compile-time type safety
	// 2. Direct JSON marshaling without interface{} conversions
	// 3. Better performance with no runtime reflection
}

// Helper function to create provider (would be configured based on your setup)
func createProvider(config gpa.Config) (*Provider, error) {
	return NewProvider(config)
}

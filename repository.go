// Package gparedis provides a Redis adapter for the Go Persistence API (GPA)
package gparedis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/lemmego/gpa"
)

// =====================================
// Generic Redis Repository Implementation
// =====================================

// RepositoryG implements type-safe Redis operations using Go generics.
// Provides compile-time type safety for all key-value operations.
type Repository[T any] struct {
	provider  *Provider
	client    *redis.Client
	keyPrefix string
}

// NewRepository creates a new generic Redis repository for type T.
// Example: userRepo := NewRepository[User](provider, client, "user:")
func NewRepository[T any](provider *Provider, client *redis.Client, keyPrefix string) *Repository[T] {
	return &Repository[T]{
		provider:  provider,
		client:    client,
		keyPrefix: keyPrefix,
	}
}

// buildKey creates a full key with the prefix
func (r *Repository[T]) buildKey(key string) string {
	if r.keyPrefix == "" {
		return key
	}
	return r.keyPrefix + key
}

// =====================================
// BasicKeyValueRepositoryG Implementation
// =====================================

// Get retrieves a value by key with compile-time type safety.
// Returns the value directly without requiring a destination parameter.
func (r *Repository[T]) Get(ctx context.Context, key string) (*T, error) {
	fullKey := r.buildKey(key)
	result := r.client.Get(ctx, fullKey)
	if err := result.Err(); err != nil {
		if err == redis.Nil {
			return nil, gpa.GPAError{
				Type:    gpa.ErrorTypeNotFound,
				Message: fmt.Sprintf("key not found: %s", key),
			}
		}
		return nil, convertRedisError(err)
	}

	data, err := result.Bytes()
	if err != nil {
		return nil, convertRedisError(err)
	}

	var entity T
	if err := json.Unmarshal(data, &entity); err != nil {
		return nil, gpa.GPAError{
			Type:    gpa.ErrorTypeSerialization,
			Message: "failed to deserialize value",
			Cause:   err,
		}
	}

	// Execute after find hook
	if hook, ok := any(&entity).(gpa.AfterFindHook); ok {
		if err := hook.AfterFind(ctx); err != nil {
			// Log error but don't fail the operation
			// log.Printf("after find hook failed: %v", err)
		}
	}

	return &entity, nil
}

// Set stores a value with compile-time type safety.
// Accepts the value directly without interface{} conversion.
func (r *Repository[T]) Set(ctx context.Context, key string, value *T) error {
	return r.SetWithTTL(ctx, key, value, 0)
}

// DeleteKey removes a key-value pair.
func (r *Repository[T]) DeleteKey(ctx context.Context, key string) error {
	// First, try to get the entity to run hooks on it
	entity, err := r.Get(ctx, key)
	if err != nil {
		// If we can't get the entity, still proceed with deletion
		// This handles the case where the key doesn't exist
		if gpaErr, ok := err.(gpa.GPAError); ok && gpaErr.Type == gpa.ErrorTypeNotFound {
			// Key doesn't exist, nothing to delete
			return nil
		}
		// For other errors, we still try to delete
	}

	// Execute before delete hook if we have the entity
	if entity != nil {
		if hook, ok := any(entity).(gpa.BeforeDeleteHook); ok {
			if err := hook.BeforeDelete(ctx); err != nil {
				return gpa.GPAError{
					Type:    gpa.ErrorTypeValidation,
					Message: "before delete hook failed",
					Cause:   err,
				}
			}
		}
	}

	fullKey := r.buildKey(key)
	result := r.client.Del(ctx, fullKey)
	if err := convertRedisError(result.Err()); err != nil {
		return err
	}

	// Execute after delete hook if we have the entity
	if entity != nil {
		if hook, ok := any(entity).(gpa.AfterDeleteHook); ok {
			if err := hook.AfterDelete(ctx); err != nil {
				// Log error but don't fail the operation
				// log.Printf("after delete hook failed: %v", err)
			}
		}
	}

	return nil
}

// KeyExists checks if a key exists in the store.
func (r *Repository[T]) KeyExists(ctx context.Context, key string) (bool, error) {
	fullKey := r.buildKey(key)
	result := r.client.Exists(ctx, fullKey)
	if err := result.Err(); err != nil {
		return false, convertRedisError(err)
	}
	return result.Val() > 0, nil
}

// =====================================
// BatchKeyValueRepositoryG Implementation
// =====================================

// MGet retrieves multiple values by their keys with compile-time type safety.
// Returns a slice of pointers to values.
func (r *Repository[T]) MGet(ctx context.Context, keys []string) (map[string]*T, error) {
	if len(keys) == 0 {
		return map[string]*T{}, nil
	}

	// Build full keys
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = r.buildKey(key)
	}

	result := r.client.MGet(ctx, fullKeys...)
	if err := result.Err(); err != nil {
		return nil, convertRedisError(err)
	}

	values := result.Val()
	entities := make(map[string]*T)

	for i, value := range values {
		if value == nil {
			// Key not found, skip
			continue
		}

		data, ok := value.(string)
		if !ok {
			return nil, gpa.NewError(gpa.ErrorTypeSerialization, "unexpected value type from Redis")
		}

		var entity T
		if err := json.Unmarshal([]byte(data), &entity); err != nil {
			return nil, gpa.NewErrorWithCause(gpa.ErrorTypeSerialization, "failed to deserialize value", err)
		}

		entities[keys[i]] = &entity
	}

	return entities, nil
}

// MSet stores multiple key-value pairs with compile-time type safety.
func (r *Repository[T]) MSet(ctx context.Context, pairs map[string]*T) error {
	if len(pairs) == 0 {
		return nil
	}

	// Convert to Redis format
	redisPairs := make([]interface{}, 0, len(pairs)*2)
	for key, value := range pairs {
		fullKey := r.buildKey(key)
		
		data, err := json.Marshal(value)
		if err != nil {
			return gpa.GPAError{
				Type:    gpa.ErrorTypeSerialization,
				Message: "failed to serialize value",
				Cause:   err,
			}
		}

		redisPairs = append(redisPairs, fullKey, data)
	}

	result := r.client.MSet(ctx, redisPairs...)
	return convertRedisError(result.Err())
}

// MDelete removes multiple keys in a single operation.
func (r *Repository[T]) MDelete(ctx context.Context, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = r.buildKey(key)
	}

	result := r.client.Del(ctx, fullKeys...)
	if err := result.Err(); err != nil {
		return 0, convertRedisError(err)
	}
	return result.Val(), nil
}

// =====================================
// TTLKeyValueRepositoryG Implementation
// =====================================

// SetWithTTL stores a value with an expiration time and compile-time type safety.
func (r *Repository[T]) SetWithTTL(ctx context.Context, key string, value *T, ttl time.Duration) error {
	// Execute before create hook
	if hook, ok := any(value).(gpa.BeforeCreateHook); ok {
		if err := hook.BeforeCreate(ctx); err != nil {
			return gpa.GPAError{
				Type:    gpa.ErrorTypeValidation,
				Message: "before create hook failed",
				Cause:   err,
			}
		}
	}

	fullKey := r.buildKey(key)
	
	data, err := json.Marshal(value)
	if err != nil {
		return gpa.GPAError{
			Type:    gpa.ErrorTypeSerialization,
			Message: "failed to serialize value",
			Cause:   err,
		}
	}

	if err := convertRedisError(r.client.Set(ctx, fullKey, data, ttl).Err()); err != nil {
		return err
	}

	// Execute after create hook
	if hook, ok := any(value).(gpa.AfterCreateHook); ok {
		if err := hook.AfterCreate(ctx); err != nil {
			// Log error but don't fail the operation
			// log.Printf("after create hook failed: %v", err)
		}
	}

	return nil
}

// Expire sets or updates the TTL for an existing key.
func (r *Repository[T]) Expire(ctx context.Context, key string, ttl time.Duration) error {
	fullKey := r.buildKey(key)
	result := r.client.Expire(ctx, fullKey, ttl)
	return convertRedisError(result.Err())
}

// TTL returns the remaining time until the key expires.
func (r *Repository[T]) TTL(ctx context.Context, key string) (time.Duration, error) {
	fullKey := r.buildKey(key)
	result := r.client.TTL(ctx, fullKey)
	if err := result.Err(); err != nil {
		return 0, convertRedisError(err)
	}
	return result.Val(), nil
}

// GetTTL returns the remaining time-to-live for a key.
func (r *Repository[T]) GetTTL(ctx context.Context, key string) (time.Duration, error) {
	return r.TTL(ctx, key)
}

// SetTTL sets or updates the TTL for an existing key.
func (r *Repository[T]) SetTTL(ctx context.Context, key string, ttl time.Duration) error {
	fullKey := r.buildKey(key)
	result := r.client.Expire(ctx, fullKey, ttl)
	if err := result.Err(); err != nil {
		return convertRedisError(err)
	}
	if !result.Val() {
		return gpa.NewError(gpa.ErrorTypeNotFound, "key not found")
	}
	return nil
}

// RemoveTTL removes the TTL from a key, making it persistent.
func (r *Repository[T]) RemoveTTL(ctx context.Context, key string) error {
	fullKey := r.buildKey(key)
	result := r.client.Persist(ctx, fullKey)
	if err := result.Err(); err != nil {
		return convertRedisError(err)
	}
	if !result.Val() {
		return gpa.NewError(gpa.ErrorTypeNotFound, "key not found")
	}
	return nil
}

// =====================================
// Numeric Operations (shared interface)
// =====================================

// Increment atomically adds delta to a numeric value.
func (r *Repository[T]) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	fullKey := r.buildKey(key)
	result := r.client.IncrBy(ctx, fullKey, delta)
	if err := result.Err(); err != nil {
		return 0, convertRedisError(err)
	}
	return result.Val(), nil
}

// Decrement atomically subtracts delta from a numeric value.
func (r *Repository[T]) Decrement(ctx context.Context, key string, delta int64) (int64, error) {
	return r.Increment(ctx, key, -delta)
}

// =====================================
// Pattern Operations (shared interface)
// =====================================

// Keys returns all keys matching the given pattern.
func (r *Repository[T]) Keys(ctx context.Context, pattern string) ([]string, error) {
	fullPattern := r.buildKey(pattern)
	result := r.client.Keys(ctx, fullPattern)
	if err := result.Err(); err != nil {
		return nil, convertRedisError(err)
	}

	keys := result.Val()
	// Remove prefix from returned keys
	if r.keyPrefix != "" {
		prefixLen := len(r.keyPrefix)
		for i, key := range keys {
			if len(key) > prefixLen && key[:prefixLen] == r.keyPrefix {
				keys[i] = key[prefixLen:]
			}
		}
	}

	return keys, nil
}

// Scan iterates through keys matching a pattern using cursor-based pagination.
func (r *Repository[T]) Scan(ctx context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error) {
	fullPattern := r.buildKey(pattern)
	result := r.client.Scan(ctx, cursor, fullPattern, count)
	if err := result.Err(); err != nil {
		return nil, 0, convertRedisError(err)
	}

	keys, newCursor := result.Val()
	
	// Remove prefix from returned keys
	if r.keyPrefix != "" {
		prefixLen := len(r.keyPrefix)
		for i, key := range keys {
			if len(key) > prefixLen && key[:prefixLen] == r.keyPrefix {
				keys[i] = key[prefixLen:]
			}
		}
	}

	return keys, newCursor, nil
}

// =====================================
// Repository Interface Methods
// =====================================

// Close closes the repository and releases any resources.
func (r *Repository[T]) Close() error {
	// Redis repositories don't need to close anything specific
	// The connection is managed by the provider
	return nil
}

// Create is not applicable for Redis key-value store
func (r *Repository[T]) Create(ctx context.Context, entity *T) error {
	return gpa.NewError(gpa.ErrorTypeUnsupported, "Create operation not supported for Redis key-value store")
}

// CreateBatch is not applicable for Redis key-value store
func (r *Repository[T]) CreateBatch(ctx context.Context, entities []*T) error {
	return gpa.NewError(gpa.ErrorTypeUnsupported, "CreateBatch operation not supported for Redis key-value store")
}

// FindByID is not applicable for Redis key-value store - use Get instead
func (r *Repository[T]) FindByID(ctx context.Context, id interface{}) (*T, error) {
	return nil, gpa.NewError(gpa.ErrorTypeUnsupported, "FindByID operation not supported for Redis key-value store - use Get instead")
}

// FindAll is not applicable for Redis key-value store
func (r *Repository[T]) FindAll(ctx context.Context, opts ...gpa.QueryOption) ([]*T, error) {
	return nil, gpa.NewError(gpa.ErrorTypeUnsupported, "FindAll operation not supported for Redis key-value store")
}

// Update is not applicable for Redis key-value store - use Set instead
func (r *Repository[T]) Update(ctx context.Context, entity *T) error {
	return gpa.NewError(gpa.ErrorTypeUnsupported, "Update operation not supported for Redis key-value store - use Set instead")
}

// UpdatePartial is not applicable for Redis key-value store
func (r *Repository[T]) UpdatePartial(ctx context.Context, id interface{}, updates map[string]interface{}) error {
	return gpa.NewError(gpa.ErrorTypeUnsupported, "UpdatePartial operation not supported for Redis key-value store")
}

// Delete is not applicable for Redis key-value store - use DeleteKey instead
func (r *Repository[T]) Delete(ctx context.Context, id interface{}) error {
	return gpa.NewError(gpa.ErrorTypeUnsupported, "Delete operation not supported for Redis key-value store - use DeleteKey instead")
}

// DeleteByCondition is not applicable for Redis key-value store
func (r *Repository[T]) DeleteByCondition(ctx context.Context, condition gpa.Condition) error {
	return gpa.NewError(gpa.ErrorTypeUnsupported, "DeleteByCondition operation not supported for Redis key-value store")
}

// Query is not applicable for Redis key-value store
func (r *Repository[T]) Query(ctx context.Context, opts ...gpa.QueryOption) ([]*T, error) {
	return nil, gpa.NewError(gpa.ErrorTypeUnsupported, "Query operation not supported for Redis key-value store")
}

// QueryOne is not applicable for Redis key-value store
func (r *Repository[T]) QueryOne(ctx context.Context, opts ...gpa.QueryOption) (*T, error) {
	return nil, gpa.NewError(gpa.ErrorTypeUnsupported, "QueryOne operation not supported for Redis key-value store")
}

// Count is not applicable for Redis key-value store
func (r *Repository[T]) Count(ctx context.Context, opts ...gpa.QueryOption) (int64, error) {
	return 0, gpa.NewError(gpa.ErrorTypeUnsupported, "Count operation not supported for Redis key-value store")
}

// Exists is not applicable for Redis key-value store - use KeyExists instead
func (r *Repository[T]) Exists(ctx context.Context, opts ...gpa.QueryOption) (bool, error) {
	return false, gpa.NewError(gpa.ErrorTypeUnsupported, "Exists operation not supported for Redis key-value store - use KeyExists instead")
}

// Transaction is not applicable for Redis key-value store
func (r *Repository[T]) Transaction(ctx context.Context, fn gpa.TransactionFunc[T]) error {
	return gpa.NewError(gpa.ErrorTypeUnsupported, "Transaction operation not supported for Redis key-value store")
}

// RawQuery is not applicable for Redis key-value store
func (r *Repository[T]) RawQuery(ctx context.Context, query string, args []interface{}) ([]*T, error) {
	return nil, gpa.NewError(gpa.ErrorTypeUnsupported, "RawQuery operation not supported for Redis key-value store")
}

// RawExec is not applicable for Redis key-value store
func (r *Repository[T]) RawExec(ctx context.Context, query string, args []interface{}) (gpa.Result, error) {
	return nil, gpa.NewError(gpa.ErrorTypeUnsupported, "RawExec operation not supported for Redis key-value store")
}

// GetEntityInfo returns basic entity information for Redis
func (r *Repository[T]) GetEntityInfo() (*gpa.EntityInfo, error) {
	var zero T
	return &gpa.EntityInfo{
		Name:       fmt.Sprintf("%T", zero),
		TableName:  r.keyPrefix,
		PrimaryKey: []string{"key"},
		Fields:     []gpa.FieldInfo{},
		Indexes:    []gpa.IndexInfo{},
		Relations:  []gpa.RelationInfo{},
	}, nil
}

// =====================================
// Helper Functions
// =====================================

// convertRedisError converts Redis errors to GPA errors
func convertRedisError(err error) error {
	if err == nil {
		return nil
	}
	if err == redis.Nil {
		return gpa.NewErrorWithCause(gpa.ErrorTypeNotFound, "key not found", err)
	}
	// If it's already a GPA error, return it as is
	if gpaErr, ok := err.(gpa.GPAError); ok {
		return gpaErr
	}
	return gpa.NewErrorWithCause(gpa.ErrorTypeDatabase, "redis error", err)
}

// NewAdvancedKVRepository creates a new type-safe advanced Redis repository.
// This repository implements all KV capabilities with compile-time type safety.
func NewAdvancedKVRepository[T any](provider *Provider, client *redis.Client, keyPrefix string) gpa.AdvancedKeyValueRepository[T] {
	return NewRepository[T](provider, client, keyPrefix)
}

// Compile-time interface checks for generic repository
var (
	_ gpa.Repository[any]                 = (*Repository[any])(nil)
	_ gpa.BasicKeyValueRepository[any]    = (*Repository[any])(nil)
	_ gpa.BatchKeyValueRepository[any]    = (*Repository[any])(nil)
	_ gpa.TTLKeyValueRepository[any]      = (*Repository[any])(nil)
	_ gpa.AdvancedKeyValueRepository[any] = (*Repository[any])(nil)
)
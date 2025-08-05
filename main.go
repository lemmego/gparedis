// Package gparedis provides a Redis adapter for the Go Persistence API (GPA)
package gparedis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/lemmego/gpa"
)

// =====================================
// Provider Implementation
// =====================================

// Provider implements gpa.Provider and gpa.KeyValueProvider using Redis
type Provider struct {
	client *redis.Client
	config gpa.Config
}

// NewProvider creates a new Redis provider instance
func NewProvider(config gpa.Config) (*Provider, error) {
	provider := &Provider{config: config}

	// Build Redis connection options
	opts, err := buildRedisOptions(config)
	if err != nil {
		return nil, err
	}

	// Apply Redis-specific options
	if options, ok := config.Options["redis"]; ok {
		if redisOptions, ok := options.(map[string]interface{}); ok {
			applyRedisOptions(opts, redisOptions)
		}
	}

	// Create Redis client
	client := redis.NewClient(opts)

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	provider.client = client
	return provider, nil
}

// Configure applies configuration to the provider
func (p *Provider) Configure(config gpa.Config) error {
	p.config = config
	return nil
}

// Health checks if the Redis connection is healthy
func (p *Provider) Health() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.client.Ping(ctx).Err()
}

// Close closes the Redis connection
func (p *Provider) Close() error {
	return p.client.Close()
}

// SupportedFeatures returns the features supported by Redis
func (p *Provider) SupportedFeatures() []gpa.Feature {
	return []gpa.Feature{
		gpa.FeatureTTL,
		gpa.FeatureAtomicOps,
		gpa.FeaturePubSub,
		gpa.FeatureStreaming,
		gpa.FeatureTransactions,
	}
}

// ProviderInfo returns information about the Redis provider
func (p *Provider) ProviderInfo() gpa.ProviderInfo {
	return gpa.ProviderInfo{
		Name:         "Redis",
		Version:      "1.0.0",
		DatabaseType: gpa.DatabaseTypeKV,
		Features:     p.SupportedFeatures(),
	}
}

// GetRepository returns a type-safe repository for any entity type T
// This enables the unified provider API: userRepo := gparedis.GetRepository[User](provider)
func GetRepository[T any](p *Provider) gpa.Repository[T] {
	return NewRepository[T](p, p.client, "")
}

// =====================================
// KeyValueProvider Implementation
// =====================================

// Client returns the underlying Redis client instance
func (p *Provider) Client() interface{} {
	return p.client
}

// Set stores a key-value pair with optional TTL
func (p *Provider) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if ttl > 0 {
		return p.client.Set(ctx, key, value, ttl).Err()
	}
	return p.client.Set(ctx, key, value, 0).Err()
}

// Get retrieves a value by key
func (p *Provider) Get(ctx context.Context, key string) (interface{}, error) {
	return p.client.Get(ctx, key).Result()
}

// Delete removes a key
func (p *Provider) Delete(ctx context.Context, key string) error {
	return p.client.Del(ctx, key).Err()
}

// Exists checks if a key exists
func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	count, err := p.client.Exists(ctx, key).Result()
	return count > 0, err
}

// Keys returns all keys matching a pattern
func (p *Provider) Keys(ctx context.Context, pattern string) ([]string, error) {
	return p.client.Keys(ctx, pattern).Result()
}

// Expire sets TTL for a key
func (p *Provider) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return p.client.Expire(ctx, key, ttl).Err()
}

// TTL returns the remaining TTL for a key
func (p *Provider) TTL(ctx context.Context, key string) (time.Duration, error) {
	return p.client.TTL(ctx, key).Result()
}


// =====================================
// Helper Functions for Tests
// =====================================

// buildRedisOptions creates Redis connection options from GPA config
func buildRedisOptions(config gpa.Config) (*redis.Options, error) {
	opts := &redis.Options{}

	// Parse connection URL if provided
	if config.ConnectionURL != "" {
		opts.Addr = "localhost:6379" // Default
		opts.DB = 0                  // Default

		// Simple URL parsing for redis://[username:password@]host[:port][/db]
		url := config.ConnectionURL
		if strings.HasPrefix(url, "redis://") {
			url = strings.TrimPrefix(url, "redis://")
		}

		// Split by @ to separate auth from host
		parts := strings.Split(url, "@")
		var hostPart string
		if len(parts) == 2 {
			// Has auth
			authPart := parts[0]
			hostPart = parts[1]
			
			// Parse username:password
			if strings.Contains(authPart, ":") {
				authParts := strings.Split(authPart, ":")
				opts.Username = authParts[0]
				opts.Password = authParts[1]
			} else {
				opts.Password = authPart
			}
		} else {
			hostPart = parts[0]
		}

		// Parse host:port/db
		if strings.Contains(hostPart, "/") {
			hostDbParts := strings.Split(hostPart, "/")
			hostPart = hostDbParts[0]
			if len(hostDbParts) > 1 && hostDbParts[1] != "" {
				if db, err := strconv.Atoi(hostDbParts[1]); err == nil {
					opts.DB = db
				}
			}
		}

		// Set address
		if hostPart != "" {
			opts.Addr = hostPart
		}
	} else {
		// Use individual parameters
		host := config.Host
		port := config.Port
		if host == "" {
			host = "localhost"
		}
		if port == 0 {
			port = 6379
		}
		opts.Addr = fmt.Sprintf("%s:%d", host, port)
		opts.Username = config.Username
		opts.Password = config.Password
		
		// Parse database number
		if config.Database != "" {
			if db, err := strconv.Atoi(config.Database); err == nil {
				opts.DB = db
			} else {
				return nil, fmt.Errorf("invalid database number: %s", config.Database)
			}
		}
	}

	// Apply connection pool settings
	if config.MaxOpenConns > 0 {
		opts.PoolSize = config.MaxOpenConns
	}
	if config.MaxIdleConns > 0 {
		opts.MinIdleConns = config.MaxIdleConns
	}

	return opts, nil
}

// applyRedisOptions applies Redis-specific options to the connection options
func applyRedisOptions(opts *redis.Options, redisOptions map[string]interface{}) {
	if maxRetries, ok := redisOptions["max_retries"]; ok {
		if retries, ok := maxRetries.(int); ok {
			opts.MaxRetries = retries
		}
	}

	if poolSize, ok := redisOptions["pool_size"]; ok {
		if size, ok := poolSize.(int); ok && size > 0 {
			opts.PoolSize = size
		}
	}

	if minIdleConns, ok := redisOptions["min_idle_conns"]; ok {
		if conns, ok := minIdleConns.(int); ok && conns >= 0 {
			opts.MinIdleConns = conns
		}
	}

	if dialTimeout, ok := redisOptions["dial_timeout"]; ok {
		if timeout, ok := dialTimeout.(time.Duration); ok {
			opts.DialTimeout = timeout
		} else if timeoutStr, ok := dialTimeout.(string); ok {
			if timeout, err := time.ParseDuration(timeoutStr); err == nil {
				opts.DialTimeout = timeout
			}
		}
	}

	if readTimeout, ok := redisOptions["read_timeout"]; ok {
		if timeout, ok := readTimeout.(time.Duration); ok {
			opts.ReadTimeout = timeout
		} else if timeoutStr, ok := readTimeout.(string); ok {
			if timeout, err := time.ParseDuration(timeoutStr); err == nil {
				opts.ReadTimeout = timeout
			}
		}
	}

	if writeTimeout, ok := redisOptions["write_timeout"]; ok {
		if timeout, ok := writeTimeout.(time.Duration); ok {
			opts.WriteTimeout = timeout
		} else if timeoutStr, ok := writeTimeout.(string); ok {
			if timeout, err := time.ParseDuration(timeoutStr); err == nil {
				opts.WriteTimeout = timeout
			}
		}
	}

	if poolTimeout, ok := redisOptions["pool_timeout"]; ok {
		if timeout, ok := poolTimeout.(time.Duration); ok {
			opts.PoolTimeout = timeout
		} else if timeoutStr, ok := poolTimeout.(string); ok {
			if timeout, err := time.ParseDuration(timeoutStr); err == nil {
				opts.PoolTimeout = timeout
			}
		}
	}
}
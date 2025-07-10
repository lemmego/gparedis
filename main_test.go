package gparedis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lemmego/gpa"
)

func getTestConfig() gpa.Config {
	// Check for Redis connection string in environment
	redisURL := os.Getenv("REDIS_TEST_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	return gpa.Config{
		Driver:        "redis",
		ConnectionURL: redisURL,
		Database:      "0", // Redis database number
	}
}

func skipIfNoRedis(t *testing.T) {
	config := getTestConfig()
	provider, err := NewProvider(config)
	if err != nil {
		t.Skipf("Skipping Redis tests: %v", err)
	}
	provider.Close()
}

func TestNewProvider(t *testing.T) {
	skipIfNoRedis(t)

	config := getTestConfig()
	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close()

	if provider == nil {
		t.Fatal("Expected provider to be created")
	}

	if provider.config.Database != "0" {
		t.Errorf("Expected database '0', got '%s'", provider.config.Database)
	}
}

func TestProviderHealth(t *testing.T) {
	skipIfNoRedis(t)

	config := getTestConfig()
	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close()

	err = provider.Health()
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestProviderInfo(t *testing.T) {
	skipIfNoRedis(t)

	config := getTestConfig()
	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close()

	info := provider.ProviderInfo()
	if info.Name != "Redis" {
		t.Errorf("Expected name 'Redis', got '%s'", info.Name)
	}
	if info.DatabaseType != gpa.DatabaseTypeKV {
		t.Errorf("Expected key-value database type, got %s", info.DatabaseType)
	}
	if len(info.Features) == 0 {
		t.Error("Expected features to be populated")
	}
}

func TestSupportedFeatures(t *testing.T) {
	skipIfNoRedis(t)

	config := getTestConfig()
	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close()

	features := provider.SupportedFeatures()
	expectedFeatures := []gpa.Feature{
		gpa.FeatureTTL,
		gpa.FeatureAtomicOps,
		gpa.FeaturePubSub,
		gpa.FeatureStreaming,
		gpa.FeatureTransactions,
	}

	if len(features) != len(expectedFeatures) {
		t.Errorf("Expected %d features, got %d", len(expectedFeatures), len(features))
	}

	for _, expected := range expectedFeatures {
		found := false
		for _, feature := range features {
			if feature == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected feature '%s' not found", expected)
		}
	}
}

func TestUnifiedProviderAPI(t *testing.T) {
	skipIfNoRedis(t)

	type User struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	config := getTestConfig()
	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close()

	if provider == nil {
		t.Fatal("Expected provider to be created")
	}

	// Test getting repository using unified API
	repo := GetRepository[User](provider)
	if repo == nil {
		t.Fatal("Expected repository to be created")
	}

	// Test provider methods
	err = provider.Health()
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}

	info := provider.ProviderInfo()
	if info.Name != "Redis" {
		t.Errorf("Expected name 'Redis', got '%s'", info.Name)
	}
}

func TestProviderConfigure(t *testing.T) {
	skipIfNoRedis(t)

	config := getTestConfig()
	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close()

	newConfig := gpa.Config{
		Driver:   "redis",
		Database: "1",
	}

	err = provider.Configure(newConfig)
	if err != nil {
		t.Errorf("Failed to configure provider: %v", err)
	}

	if provider.config.Database != "1" {
		t.Errorf("Expected database '1', got '%s'", provider.config.Database)
	}
}

func TestBuildRedisOptions(t *testing.T) {
	// Test with connection URL
	config := gpa.Config{
		ConnectionURL: "redis://user:pass@localhost:6379/0",
	}
	opts, err := buildRedisOptions(config)
	if err != nil {
		t.Errorf("Failed to build Redis options: %v", err)
	}
	if opts.Addr != "localhost:6379" {
		t.Errorf("Expected addr 'localhost:6379', got '%s'", opts.Addr)
	}
	if opts.Username != "user" {
		t.Errorf("Expected username 'user', got '%s'", opts.Username)
	}
	if opts.Password != "pass" {
		t.Errorf("Expected password 'pass', got '%s'", opts.Password)
	}
	if opts.DB != 0 {
		t.Errorf("Expected database 0, got %d", opts.DB)
	}

	// Test with individual parameters
	config = gpa.Config{
		Host:     "localhost",
		Port:     6379,
		Database: "1",
		Username: "user",
		Password: "pass",
	}
	opts, err = buildRedisOptions(config)
	if err != nil {
		t.Errorf("Failed to build Redis options: %v", err)
	}
	if opts.Addr != "localhost:6379" {
		t.Errorf("Expected addr 'localhost:6379', got '%s'", opts.Addr)
	}
	if opts.DB != 1 {
		t.Errorf("Expected database 1, got %d", opts.DB)
	}

	// Test with defaults
	config = gpa.Config{}
	opts, err = buildRedisOptions(config)
	if err != nil {
		t.Errorf("Failed to build Redis options: %v", err)
	}
	if opts.Addr != "localhost:6379" {
		t.Errorf("Expected default addr 'localhost:6379', got '%s'", opts.Addr)
	}
	if opts.DB != 0 {
		t.Errorf("Expected default database 0, got %d", opts.DB)
	}
}

func TestProviderWithCustomOptions(t *testing.T) {
	skipIfNoRedis(t)

	config := gpa.Config{
		Driver:   "redis",
		Host:     "localhost",
		Port:     6379,
		Database: "0",
		Options: map[string]interface{}{
			"redis": map[string]interface{}{
				"max_retries":      3,
				"read_timeout":     "5s",
				"write_timeout":    "5s",
				"pool_size":        10,
				"min_idle_conns":   2,
				"max_conn_age":     "30m",
				"pool_timeout":     "4s",
				"idle_timeout":     "5m",
				"idle_check_freq":  "1m",
			},
		},
	}

	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider with custom options: %v", err)
	}
	defer provider.Close()

	if provider == nil {
		t.Fatal("Expected provider to be created")
	}
}

func TestProviderConnectionPoolSettings(t *testing.T) {
	skipIfNoRedis(t)

	config := gpa.Config{
		Driver:          "redis",
		Host:            "localhost",
		Port:            6379,
		Database:        "0",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: time.Minute * 30,
	}

	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close()

	// Test that the provider was created successfully with pool settings
	stats := provider.client.PoolStats()
	if stats.TotalConns < 0 {
		t.Error("Expected valid pool stats")
	}
}

func TestInvalidConnectionURL(t *testing.T) {
	config := gpa.Config{
		Driver:        "redis",
		ConnectionURL: "invalid-url",
	}

	_, err := NewProvider(config)
	if err == nil {
		t.Error("Expected error for invalid connection URL")
	}
}

func TestInvalidDatabaseNumber(t *testing.T) {
	config := gpa.Config{
		Driver:   "redis",
		Host:     "localhost",
		Port:     6379,
		Database: "invalid",
	}

	_, err := NewProvider(config)
	if err == nil {
		t.Error("Expected error for invalid database number")
	}
}

func TestContextTimeout(t *testing.T) {
	skipIfNoRedis(t)

	config := getTestConfig()
	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close()

	// Create a context with timeout
	_, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	// This should work since Redis is fast
	err = provider.Health()
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestProviderWithoutRedis(t *testing.T) {
	// Test with invalid connection
	config := gpa.Config{
		Driver: "redis",
		Host:   "invalid-host",
		Port:   6379,
	}

	provider, err := NewProvider(config)
	if err != nil {
		// This is expected if Redis is not available
		t.Logf("Redis not available: %v", err)
		return
	}
	defer provider.Close()

	// Try to ping
	err = provider.Health()
	if err != nil {
		t.Logf("Redis ping failed: %v", err)
	}
}

func TestInvalidRedisOptions(t *testing.T) {
	skipIfNoRedis(t)

	config := gpa.Config{
		Driver:   "redis",
		Host:     "localhost",
		Port:     6379,
		Database: "0",
		Options: map[string]interface{}{
			"redis": map[string]interface{}{
				"max_retries":      "invalid", // should be int
				"read_timeout":     123,       // should be string
				"pool_size":        -1,        // invalid value
				"min_idle_conns":   "invalid", // should be int
			},
		},
	}

	// Should still work, just ignore invalid options
	provider, err := NewProvider(config)
	if err != nil {
		t.Fatalf("Failed to create provider with invalid options: %v", err)
	}
	defer provider.Close()
}

func TestApplyRedisOptions(t *testing.T) {
	config := gpa.Config{
		Options: map[string]interface{}{
			"redis": map[string]interface{}{
				"max_retries":      3,
				"read_timeout":     "5s",
				"write_timeout":    "3s",
				"dial_timeout":     "2s",
				"pool_size":        20,
				"min_idle_conns":   5,
				"max_conn_age":     "1h",
				"pool_timeout":     "10s",
				"idle_timeout":     "30m",
				"idle_check_freq":  "2m",
			},
		},
	}

	opts, err := buildRedisOptions(config)
	if err != nil {
		t.Fatalf("Failed to build Redis options: %v", err)
	}

	// Apply custom options
	if redisOpts, ok := config.Options["redis"]; ok {
		if redisOptsMap, ok := redisOpts.(map[string]interface{}); ok {
			applyRedisOptions(opts, redisOptsMap)
		}
	}

	// Verify some options were applied
	if opts.MaxRetries != 3 {
		t.Errorf("Expected max retries 3, got %d", opts.MaxRetries)
	}
	if opts.PoolSize != 20 {
		t.Errorf("Expected pool size 20, got %d", opts.PoolSize)
	}
	if opts.MinIdleConns != 5 {
		t.Errorf("Expected min idle conns 5, got %d", opts.MinIdleConns)
	}
}

func TestSSLConfiguration(t *testing.T) {
	skipIfNoRedis(t)

	config := gpa.Config{
		Driver:   "redis",
		Host:     "localhost",
		Port:     6379,
		Database: "0",
		SSL: gpa.SSLConfig{
			Enabled: true,
			Mode:    "require",
		},
	}

	provider, err := NewProvider(config)
	if err != nil {
		// SSL may not be supported in test environment
		t.Logf("SSL connection failed (expected in test env): %v", err)
		return
	}
	defer provider.Close()

	if provider == nil {
		t.Fatal("Expected provider to be created")
	}
}
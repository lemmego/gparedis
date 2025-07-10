# GPARedis

A Redis adapter for the Go Persistence API (GPA), providing type-safe Redis operations with compile-time type safety using Go generics.

## Features

- **Type-safe operations**: All key-value operations are type-safe with compile-time checking
- **GPA compliance**: Implements the Go Persistence API interfaces
- **Redis-specific features**: TTL support, atomic operations, pattern matching
- **Batch operations**: Support for multi-get, multi-set, and multi-delete operations
- **Flexible configuration**: Support for connection URLs and individual parameters
- **Connection management**: Built-in connection pooling and health checks

## Installation

```bash
go get github.com/lemmego/gparedis
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/lemmego/gpa"
    "github.com/lemmego/gparedis"
)

type User struct {
    ID   string `json:"id"`
    Name string `json:"name"`
    Age  int    `json:"age"`
}

func main() {
    // Create Redis provider
    config := gpa.Config{
        Host:     "localhost",
        Port:     6379,
        Database: "0",
    }
    
    provider, err := gparedis.NewProvider(config)
    if err != nil {
        log.Fatal(err)
    }
    defer provider.Close()
    
    // Get type-safe repository
    userRepo := gparedis.GetRepository[User](provider)
    
    // Store a user
    user := &User{ID: "123", Name: "John Doe", Age: 30}
    if err := userRepo.Set(context.Background(), "user:123", user); err != nil {
        log.Fatal(err)
    }
    
    // Retrieve a user
    retrievedUser, err := userRepo.Get(context.Background(), "user:123")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Retrieved user: %+v\n", retrievedUser)
}
```

## Configuration

### Using Connection URL

```go
config := gpa.Config{
    ConnectionURL: "redis://username:password@localhost:6379/0",
}
```

### Using Individual Parameters

```go
config := gpa.Config{
    Host:     "localhost",
    Port:     6379,
    Username: "username",
    Password: "password",
    Database: "0",
}
```

### Redis-specific Options

```go
config := gpa.Config{
    Host: "localhost",
    Port: 6379,
    Options: map[string]interface{}{
        "redis": map[string]interface{}{
            "max_retries":     3,
            "pool_size":       10,
            "min_idle_conns":  5,
            "dial_timeout":    "5s",
            "read_timeout":    "3s",
            "write_timeout":   "3s",
            "pool_timeout":    "4s",
        },
    },
}
```

## Supported Operations

### Basic Key-Value Operations

- `Get(ctx, key)` - Retrieve a value by key
- `Set(ctx, key, value)` - Store a value
- `DeleteKey(ctx, key)` - Delete a key
- `KeyExists(ctx, key)` - Check if key exists

### Batch Operations

- `MGet(ctx, keys)` - Get multiple values
- `MSet(ctx, pairs)` - Set multiple key-value pairs
- `MDelete(ctx, keys)` - Delete multiple keys

### TTL Operations

- `SetWithTTL(ctx, key, value, ttl)` - Store with expiration
- `SetTTL(ctx, key, ttl)` - Set TTL for existing key
- `GetTTL(ctx, key)` - Get remaining TTL
- `RemoveTTL(ctx, key)` - Remove TTL (make persistent)

### Atomic Operations

- `Increment(ctx, key, delta)` - Atomic increment
- `Decrement(ctx, key, delta)` - Atomic decrement

### Pattern Operations

- `Keys(ctx, pattern)` - Get keys matching pattern
- `Scan(ctx, cursor, pattern, count)` - Scan keys with cursor

## Supported Features

- **TTL**: Time-to-live support for keys
- **Atomic Operations**: Atomic increment/decrement operations
- **Pub/Sub**: Redis pub/sub capabilities
- **Streaming**: Redis streams support
- **Transactions**: Redis transaction support

## Error Handling

The package converts Redis errors to GPA errors with proper error types:

- `ErrorTypeNotFound` - Key not found
- `ErrorTypeSerialization` - JSON serialization/deserialization errors
- `ErrorTypeDatabase` - General Redis errors

## License

MIT License - see [LICENSE.md](LICENSE.md) for details.
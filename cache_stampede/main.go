package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// Client represents a TCP client connection to the cache server
type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	id     int
}

// NewClient creates a new client connection to the server
func NewClient(ctx context.Context, id int, serverAddr string) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect client %d: %w", id, err)
	}

	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		id:     id,
	}, nil
}

// SendRequest sends a request to the server and returns the response
func (c *Client) SendRequest(ctx context.Context, key string) (string, error) {
	// Send request
	_, err := fmt.Fprintf(c.conn, "%s\n", key)
	if err != nil {
		return "", fmt.Errorf("client %d failed to send request: %w", c.id, err)
	}

	// Read response
	response, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("client %d failed to read response: %w", c.id, err)
	}

	return response, nil
}

// Close closes the client connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// CacheStampedeDemo demonstrates cache stampede with concurrent requests
func CacheStampedeDemo(ctx context.Context, serverAddr string, numClients int, targetKey string) {
	fmt.Printf("🏃‍♂️ Starting Cache Stampede Demo\n")
	fmt.Printf("   📊 Clients: %d\n", numClients)
	fmt.Printf("   🎯 Target Key: %s\n", targetKey)
	fmt.Printf("   🌐 Server: %s\n", serverAddr)
	fmt.Println("=" + fmt.Sprintf("%60s", "="))

	var wg sync.WaitGroup
	results := make(chan string, numClients)

	// Launch concurrent clients
	for i := 1; i <= numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// Create client connection
			client, err := NewClient(ctx, clientID, serverAddr)
			if err != nil {
				fmt.Printf("❌ Client %d connection failed: %v\n", clientID, err)
				return
			}
			defer client.Close()

			fmt.Printf("🔗 Client %d connected\n", clientID)

			// Send request for the same key (this will cause cache stampede)
			start := time.Now()
			response, err := client.SendRequest(ctx, targetKey)
			duration := time.Since(start)

			if err != nil {
				fmt.Printf("❌ Client %d request failed: %v\n", clientID, err)
				return
			}

			result := fmt.Sprintf("✅ Client %d received: %s (took %v)",
				clientID, response[:min(50, len(response))], duration)
			fmt.Println(result)
			results <- result
		}(i)
	}

	// Wait for all clients to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var responses []string
	for result := range results {
		responses = append(responses, result)
	}

	fmt.Println("=" + fmt.Sprintf("%60s", "="))
	fmt.Printf("🎉 Cache Stampede Demo Complete! %d responses received\n", len(responses))
}

// WarmUpCache sends a request to warm up the cache before the stampede
func WarmUpCache(ctx context.Context, serverAddr string, key string) error {
	client, err := NewClient(ctx, 0, serverAddr)
	if err != nil {
		return fmt.Errorf("failed to create warmup client: %w", err)
	}
	defer client.Close()

	fmt.Printf("🔥 Warming up cache for key '%s'...\n", key)
	response, err := client.SendRequest(ctx, key)
	if err != nil {
		return fmt.Errorf("warmup failed: %w", err)
	}

	fmt.Printf("✅ Cache warmed up: %s\n", response[:min(50, len(response))])
	return nil
}

func main() {
	ctx := context.Background()
	serverAddr := "localhost:8000"

	fmt.Println("🚀 Cache Stampede Reproduction Tool")
	fmt.Println("This Go client will send concurrent requests to demonstrate cache stampede")
	fmt.Println()

	// Wait a moment for server to be ready
	fmt.Println("⏱️  Waiting 2 seconds for server to be ready...")
	time.Sleep(2 * time.Second)

	// Test connection to server
	fmt.Println("🔍 Testing connection to server...")
	testClient, err := NewClient(ctx, -1, serverAddr)
	if err != nil {
		fmt.Printf("❌ Cannot connect to server at %s: %v\n", serverAddr, err)
		fmt.Println("💡 Make sure to run the Python server first: python3 server.py")
		return
	}
	testClient.Close()
	fmt.Println("✅ Server connection successful!")
	fmt.Println()

	// Demo 1: Cache Stampede - multiple requests for uncached key
	fmt.Println("🎯 DEMO 1: Cache Stampede (cold cache)")
	CacheStampedeDemo(ctx, serverAddr, 5, "stampede_key")

	fmt.Println()
	time.Sleep(1 * time.Second)

	// Demo 2: Cache Hit - requests for already cached key
	fmt.Println("🎯 DEMO 2: Cache Hits (warm cache)")
	CacheStampedeDemo(ctx, serverAddr, 5, "stampede_key")

	fmt.Println()
	time.Sleep(1 * time.Second)

	// Demo 3: Another stampede with different key
	fmt.Println("🎯 DEMO 3: Another Cache Stampede (different key)")
	CacheStampedeDemo(ctx, serverAddr, 8, "another_stampede_key")

	fmt.Println()
	fmt.Println("🎉 All demos completed!")
	fmt.Println("💡 Check the Python server output to see the cache stampede happening!")
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

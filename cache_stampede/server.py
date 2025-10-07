#!/usr/bin/env python3
import socket
import threading
import time
import json
from typing import Dict, Optional

class CacheAsideServer:
    def __init__(self, host: str = 'localhost', port: int = 8000):
        self.host = host
        self.port = port
        self.cache: Dict[str, str] = {}
        self.cache_lock = threading.Lock()

    def expensive_operation(self, key: str) -> str:
        """Simulate an expensive database or API call"""
        print(f"🔥 CACHE MISS: Performing expensive operation for key '{key}'")
        print("   ⏱️  Simulating 2-second database call...")
        time.sleep(2)  # Simulate expensive operation
        result = f"expensive_result_for_{key}_{int(time.time())}"
        print(f"   ✅ Expensive operation completed for key '{key}': {result}")
        return result

    def get_from_cache(self, key: str) -> Optional[str]:
        """Cache-aside pattern: check cache first"""
        with self.cache_lock:
            if key in self.cache:
                print(f"💰 CACHE HIT: Found '{key}' in cache")
                return self.cache[key]
            else:
                print(f"💸 CACHE MISS: Key '{key}' not found in cache")
                return None

    def set_cache(self, key: str, value: str):
        """Set value in cache"""
        with self.cache_lock:
            self.cache[key] = value
            print(f"📝 CACHE SET: Stored '{key}' -> '{value}' in cache")

    def handle_request(self, key: str) -> str:
        """Cache-aside pattern implementation"""
        # Step 1: Try to get from cache
        cached_value = self.get_from_cache(key)

        if cached_value is not None:
            return cached_value

        # Step 2: Cache miss - get from "database" (expensive operation)
        # This is where cache stampede happens: multiple requests for the same key
        # will all miss the cache and perform the expensive operation simultaneously
        fresh_value = self.expensive_operation(key)

        # Step 3: Store in cache for future requests
        self.set_cache(key, fresh_value)

        return fresh_value

    def handle_client(self, client_socket: socket.socket, client_address):
        """Handle individual client connection"""
        print(f"🔗 New connection from {client_address}")

        try:
            while True:
                # Receive data from client
                data = client_socket.recv(1024).decode('utf-8').strip()
                if not data:
                    break

                print(f"📨 Received request for key: '{data}' from {client_address}")

                # Process the request using cache-aside pattern
                result = self.handle_request(data)

                # Send response back to client
                response = f"{result}\n"
                client_socket.send(response.encode('utf-8'))
                print(f"📤 Sent response to {client_address}: {result}")

        except Exception as e:
            print(f"❌ Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            print(f"🔌 Connection closed for {client_address}")

    def start_server(self):
        """Start the TCP server"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(10)
            print(f"🚀 Cache server started on {self.host}:{self.port}")
            print(f"📊 Cache contents: {self.cache}")
            print("=" * 60)

            while True:
                client_socket, client_address = server_socket.accept()

                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True
                client_thread.start()

        except KeyboardInterrupt:
            print("\n🛑 Server shutting down...")
        finally:
            server_socket.close()

if __name__ == "__main__":
    print("🏭 Starting Cache-Aside TCP Server")
    print("This server demonstrates cache stampede when multiple")
    print("concurrent requests ask for the same uncached key.")
    print("=" * 60)

    server = CacheAsideServer()
    server.start_server()

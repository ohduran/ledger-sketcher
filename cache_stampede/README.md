## To run the demonstration:

1. Start the Python server:
```bash
cd cache_stampede
python3 server.py
```

2. In another terminal, run the Go client:
```bash
cd cache_stampede
go run main.go
```

The Python server will show detailed prints demonstrating the cache stampede - multiple concurrent requests for the same uncached key will all trigger the expensive operation simultaneously, showing the problem with naive cache-aside implementation.

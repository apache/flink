# Flink Blocklist REST API Usage Guide

This document describes how to use the Flink Blocklist REST API to manage blocked nodes in a Flink cluster.

## Overview

The Blocklist API allows you to:
- List all currently blocked nodes
- Add nodes to the blocklist to prevent new slot allocations
- Remove nodes from the blocklist to allow slot allocations again

This functionality is independent of speculative execution and can be used for manual node management.

## Configuration

Enable the blocklist functionality by setting:

```yaml
execution.batch.blocklist.enabled: true
execution.batch.blocklist.default-duration: 10m
execution.batch.blocklist.max-duration: 24h
```

## API Endpoints

### 1. List Blocked Nodes

**GET** `/cluster/blocklist`

Returns a list of all currently blocked nodes.

**Example Response:**
```json
{
  "blockedNodes": [
    {
      "nodeId": "node-1",
      "reason": "High memory usage",
      "blockedUntil": "2024-01-01T12:00:00Z"
    }
  ]
}
```

### 2. Add Node to Blocklist

**POST** `/cluster/blocklist`

Adds a node to the blocklist.

**Request Body:**
```json
{
  "nodeId": "node-1",
  "reason": "High memory usage",
  "duration": "PT30M"
}
```

**Example Response:**
```json
{
  "success": true,
  "message": "Node node-1 has been added to the blocklist"
}
```

### 3. Remove Node from Blocklist

**DELETE** `/cluster/blocklist/{nodeId}`

Removes a node from the blocklist.

**Example Response:**
```json
{
  "success": true,
  "message": "Node node-1 has been removed from the blocklist"
}
```

## Usage Examples

### Using curl

```bash
# List blocked nodes
curl -X GET http://localhost:8081/cluster/blocklist

# Add a node to blocklist for 30 minutes
curl -X POST http://localhost:8081/cluster/blocklist \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "taskmanager-1",
    "reason": "Maintenance required",
    "duration": "PT30M"
  }'

# Remove a node from blocklist
curl -X DELETE http://localhost:8081/cluster/blocklist/taskmanager-1
```

### Integration with Monitoring

The blocklist API can be integrated with monitoring systems to automatically manage problematic nodes:

```bash
#!/bin/bash
# Example script to block nodes with high error rates

NODE_ID="$1"
REASON="$2"
DURATION="${3:-PT10M}"

curl -X POST http://localhost:8081/cluster/blocklist \
  -H "Content-Type: application/json" \
  -d "{
    \"nodeId\": \"$NODE_ID\",
    \"reason\": \"$REASON\",
    \"duration\": \"$DURATION\"
  }"
```

## Notes

- Blocked nodes will not receive new slot allocations
- Existing slots on blocked nodes remain active
- Nodes are automatically unblocked when the duration expires
- The blocklist is independent of the Node Quarantine functionality
- Duration format follows ISO 8601 (e.g., "PT30M" for 30 minutes, "PT2H" for 2 hours)
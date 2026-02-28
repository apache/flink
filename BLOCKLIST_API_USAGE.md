# Flink Management Blocklist REST API Usage Guide

This document describes how to use the Flink Management Blocklist REST API to manage blocked nodes in a Flink cluster.

## Overview

The Management Blocklist API allows you to:
- List all currently blocked nodes in the management blocklist
- Add nodes to the management blocklist to prevent new slot allocations
- Remove nodes from the management blocklist to allow slot allocations again

**Important**: This management blocklist is **independent** of the batch execution blocklist used by speculative execution. The two systems serve different purposes:

- **Management Blocklist**: Manual cluster management via REST API for operational purposes
- **Batch Execution Blocklist**: Automatic blocking of slow nodes during speculative execution in batch jobs

## Configuration

Enable the management blocklist functionality by setting:

```yaml
cluster.management.blocklist.enabled: true
cluster.management.blocklist.default-duration: 10m
cluster.management.blocklist.max-duration: 24h
```

Note: These are separate from batch execution blocklist configurations (`execution.batch.speculative.*`).

## API Endpoints

### 1. List Blocked Nodes

**GET** `/cluster/blocklist`

Returns a list of all currently blocked nodes in the management blocklist.

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

### 2. Add Node to Management Blocklist

**POST** `/cluster/blocklist`

Adds a node to the management blocklist.

**Request Body:**
```json
{
  "nodeId": "node-1",
  "reason": "High memory usage",
  "duration": "PT30M"
}
```

Note: `duration` is optional. If not provided, the default duration from configuration will be used.

**Example Response:**
```json
{
  "message": "Node node-1 successfully added to management blocklist for PT30M"
}
```

### 3. Remove Node from Management Blocklist

**DELETE** `/cluster/blocklist/{nodeId}`

Removes a node from the management blocklist.

**Example Response:**
```json
{
  "message": "Node node-1 successfully removed from management blocklist"
}
```

## Usage Examples

### Using curl

```bash
# List blocked nodes
curl -X GET http://localhost:8081/cluster/blocklist

# Add a node to management blocklist for 30 minutes
curl -X POST http://localhost:8081/cluster/blocklist \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "taskmanager-1",
    "reason": "Maintenance required",
    "duration": "PT30M"
  }'

# Add a node using default duration (from configuration)
curl -X POST http://localhost:8081/cluster/blocklist \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "taskmanager-2",
    "reason": "Hardware issues"
  }'

# Remove a node from management blocklist
curl -X DELETE http://localhost:8081/cluster/blocklist/taskmanager-1
```

### Integration with Monitoring

The management blocklist API can be integrated with monitoring systems to automatically manage problematic nodes:

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

## Differences from Batch Execution Blocklist

| Feature | Management Blocklist | Batch Execution Blocklist |
|---------|---------------------|---------------------------|
| **Purpose** | Manual cluster management | Automatic speculative execution optimization |
| **Trigger** | REST API calls | Slow task detection |
| **Configuration** | `cluster.management.blocklist.*` | `execution.batch.speculative.*` |
| **Scope** | Cluster-wide, persistent | Job-specific, temporary |
| **Control** | Manual via API | Automatic by scheduler |

## Notes

- Management blocked nodes will not receive new slot allocations
- Existing slots on management blocked nodes remain active
- Nodes are automatically unblocked when the duration expires
- The management blocklist is completely independent of Node Quarantine functionality
- Duration format follows ISO 8601 (e.g., "PT30M" for 30 minutes, "PT2H" for 2 hours)
- Maximum duration is enforced by configuration to prevent accidental long-term blocking
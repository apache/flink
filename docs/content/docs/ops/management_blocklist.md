---
title: "Management Blocklist"
weight: 9
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Management Blocklist

The Management Blocklist is a feature that allows cluster administrators to temporarily block problematic nodes from receiving new task allocations. This is useful for maintenance, troubleshooting, or handling nodes with performance issues without completely removing them from the cluster.

## Overview

The Management Blocklist provides a way to:
- **Block nodes**: Prevent new slots from being allocated on specific nodes
- **Unblock nodes**: Allow previously blocked nodes to receive new allocations again
- **View blocked nodes**: List all currently blocked nodes with their block reasons and expiration times
- **Automatic cleanup**: Blocked nodes are automatically unblocked after a specified duration

Unlike the [speculative execution blocklist]({{< ref "docs/deployment/speculative_execution" >}}), the Management Blocklist is manually controlled by administrators and is intended for operational management rather than automatic performance optimization.

## Configuration

The Management Blocklist can be configured using the following options:

| Configuration Key | Default | Description |
|-------------------|---------|-------------|
| `management.blocklist.enabled` | `false` | Whether to enable the management blocklist feature |
| `management.blocklist.default-duration` | `1h` | Default duration for blocking nodes when no explicit duration is provided |
| `management.blocklist.max-duration` | `24h` | Maximum allowed duration for blocking nodes |

Example configuration:
```yaml
management:
  blocklist:
    enabled: true
    default-duration: 30min
    max-duration: 12h
```

## REST API

The Management Blocklist provides a REST API for programmatic control:

### Add Node to Blocklist

**Endpoint**: `POST /cluster/blocklist`

**Request Body**:
```json
{
  "nodeId": "taskmanager-1",
  "reason": "Maintenance required",
  "duration": "PT2H"
}
```

**Parameters**:
- `nodeId` (required): The ID of the node to block
- `reason` (required): Human-readable reason for blocking the node
- `duration` (optional): Duration to block the node (ISO 8601 format). If not provided, uses the configured default duration.

**Response**:
```json
{
  "status": "success",
  "message": "Node taskmanager-1 has been added to the blocklist"
}
```

### Remove Node from Blocklist

**Endpoint**: `DELETE /cluster/blocklist/{nodeId}`

**Parameters**:
- `nodeId` (path parameter): The ID of the node to unblock

**Response**:
```json
{
  "status": "success", 
  "message": "Node taskmanager-1 has been removed from the blocklist"
}
```

### List Blocked Nodes

**Endpoint**: `GET /cluster/blocklist`

**Response**:
```json
{
  "blockedNodes": [
    {
      "nodeId": "taskmanager-1",
      "cause": "Maintenance required",
      "blockedAt": "2024-03-15T10:30:00Z",
      "expiresAt": "2024-03-15T12:30:00Z"
    },
    {
      "nodeId": "taskmanager-3", 
      "cause": "Performance issues",
      "blockedAt": "2024-03-15T11:00:00Z",
      "expiresAt": "2024-03-15T13:00:00Z"
    }
  ]
}
```

## Usage Examples

### Using curl

```bash
# Add a node to blocklist for 2 hours
curl -X POST http://localhost:8081/cluster/blocklist \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "taskmanager-1",
    "reason": "Scheduled maintenance", 
    "duration": "PT2H"
  }'

# List all blocked nodes
curl http://localhost:8081/cluster/blocklist

# Remove a node from blocklist
curl -X DELETE http://localhost:8081/cluster/blocklist/taskmanager-1
```

### Using Flink CLI (if available)

```bash
# Add node to blocklist
flink cluster blocklist add taskmanager-1 "Maintenance required" --duration 2h

# List blocked nodes  
flink cluster blocklist list

# Remove node from blocklist
flink cluster blocklist remove taskmanager-1
```

## Behavior and Limitations

### Blocking Behavior
- **New allocations**: Blocked nodes will not receive new slot allocations
- **Existing tasks**: Tasks already running on blocked nodes continue to run normally
- **Automatic expiration**: Nodes are automatically unblocked after the specified duration
- **Immediate effect**: Blocking takes effect immediately after the API call

### Limitations
- **Cluster stability**: Blocking too many nodes may cause resource shortage
- **Running jobs**: Existing tasks on blocked nodes are not affected
- **Persistence**: Blocklist state is not persisted across JobManager restarts
- **Network partitions**: Blocked nodes that become unreachable are handled by Flink's failure detection

### Best Practices
1. **Gradual blocking**: Block nodes gradually to avoid resource shortage
2. **Monitor resources**: Ensure sufficient resources remain available after blocking
3. **Clear reasons**: Provide clear, descriptive reasons for blocking nodes
4. **Reasonable durations**: Use appropriate block durations based on maintenance needs
5. **Coordination**: Coordinate with job owners before blocking nodes with running tasks

## Integration with Other Features

### Speculative Execution
The Management Blocklist works alongside [speculative execution]({{< ref "docs/deployment/speculative_execution" >}}). Nodes blocked by either mechanism will not receive new allocations.

### Adaptive Scheduler  
The [Adaptive Scheduler]({{< ref "docs/deployment/elastic_scaling" >}}) respects the Management Blocklist when making scaling decisions.

### High Availability
In HA setups, the blocklist state is managed by the active JobManager. When a new JobManager becomes active, the blocklist state is not automatically restored and must be reconfigured if needed.

## Troubleshooting

### Common Issues

**Node not getting blocked**:
- Verify the node ID is correct
- Check that `management.blocklist.enabled` is set to `true`
- Ensure the REST API request is properly formatted

**Resource shortage after blocking**:
- Check available resources with `GET /overview`
- Consider unblocking some nodes or scaling up the cluster
- Review job resource requirements

**Blocklist not persisting**:
- The Management Blocklist state is not persisted across restarts
- Reconfigure blocked nodes after JobManager restart if needed
- Consider implementing external automation for persistent blocking

### Monitoring

Monitor the Management Blocklist through:
- REST API: `GET /cluster/blocklist` 
- Flink Web UI: Cluster overview page (if UI support is available)
- Logs: Check JobManager logs for blocklist operations
- Metrics: Monitor cluster resource utilization after blocking nodes
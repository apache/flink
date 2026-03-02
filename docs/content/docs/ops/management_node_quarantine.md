---
title: "Management Node Quarantine"
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

# Management Node Quarantine

The Management Node Quarantine is a feature that allows cluster administrators to temporarily quarantine problematic nodes from receiving new task allocations. This is useful for maintenance, troubleshooting, or handling nodes with performance issues without completely removing them from the cluster.

## Overview

The Management Node Quarantine provides a way to:
- **Quarantine nodes**: Prevent new slots from being allocated on specific nodes
- **Remove quarantine**: Allow previously quarantined nodes to receive new allocations again
- **View quarantined nodes**: List all currently quarantined nodes with their reasons and expiration times
- **Automatic cleanup**: Quarantined nodes are automatically released after a specified duration

Unlike the [speculative execution blocklist]({{< ref "docs/deployment/speculative_execution" >}}), the Management Node Quarantine is manually controlled by administrators and is intended for operational management rather than automatic performance optimization.

## Configuration

The Management Node Quarantine can be configured using the following options:

| Configuration Key | Default | Description |
|-------------------|---------|-------------|
| `cluster.management.node-quarantine.enabled` | `false` | Whether to enable the management node quarantine feature |
| `cluster.management.node-quarantine.default-duration` | `10min` | Default duration for quarantining nodes when no explicit duration is provided |
| `cluster.management.node-quarantine.max-duration` | `24h` | Maximum allowed duration for quarantining nodes |

Example configuration:
```yaml
cluster:
  management:
    node-quarantine:
      enabled: true
      default-duration: 30min
      max-duration: 12h
```

## REST API

The Management Node Quarantine provides a REST API for programmatic control:

### Add Node to Quarantine

**Endpoint**: `POST /cluster/node-quarantine`

**Request Body**:
```json
{
  "nodeId": "taskmanager-1",
  "reason": "Maintenance required",
  "duration": "PT2H"
}
```

**Parameters**:
- `nodeId` (required): The ID of the node to quarantine
- `reason` (required): Human-readable reason for quarantining the node
- `duration` (optional): Duration to quarantine the node (ISO 8601 format). If not provided, uses the configured default duration.

**Response**:
```json
{
  "status": "success",
  "message": "Node taskmanager-1 has been added to the quarantine list"
}
```

### Remove Node from Quarantine

**Endpoint**: `DELETE /cluster/node-quarantine/{nodeId}`

**Parameters**:
- `nodeId` (path parameter): The ID of the node to remove from quarantine

**Response**:
```json
{
  "status": "success",
  "message": "Node taskmanager-1 has been removed from the quarantine list"
}
```

### List Quarantined Nodes

**Endpoint**: `GET /cluster/node-quarantine`

**Response**:
```json
{
  "quarantinedNodes": [
    {
      "nodeId": "taskmanager-1",
      "cause": "Maintenance required",
      "endTimestamp": 1710503400000
    },
    {
      "nodeId": "taskmanager-3",
      "cause": "Performance issues",
      "endTimestamp": 1710510600000
    }
  ]
}
```

## Usage Examples

### Using curl

```bash
# Add a node to quarantine for 2 hours
curl -X POST http://localhost:8081/cluster/node-quarantine \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "taskmanager-1",
    "reason": "Scheduled maintenance",
    "duration": "PT2H"
  }'

# List all quarantined nodes
curl http://localhost:8081/cluster/node-quarantine

# Remove a node from quarantine
curl -X DELETE http://localhost:8081/cluster/node-quarantine/taskmanager-1
```

## Behavior and Limitations

### Quarantine Behavior
- **New allocations**: Quarantined nodes will not receive new slot allocations
- **Existing tasks**: Tasks already running on quarantined nodes continue to run normally
- **Automatic expiration**: Nodes are automatically released from quarantine after the specified duration
- **Immediate effect**: Quarantine takes effect immediately after the API call

### Limitations
- **Cluster stability**: Quarantining too many nodes may cause resource shortage
- **Running jobs**: Existing tasks on quarantined nodes are not affected
- **Persistence**: Quarantine state is not persisted across JobManager restarts
- **Network partitions**: Quarantined nodes that become unreachable are handled by Flink's failure detection

### Best Practices
1. **Gradual quarantine**: Quarantine nodes gradually to avoid resource shortage
2. **Monitor resources**: Ensure sufficient resources remain available after quarantine
3. **Clear reasons**: Provide clear, descriptive reasons for quarantining nodes
4. **Reasonable durations**: Use appropriate quarantine durations based on maintenance needs
5. **Coordination**: Coordinate with job owners before quarantining nodes with running tasks

## Integration with Other Features

### Speculative Execution
The Management Node Quarantine works alongside [speculative execution]({{< ref "docs/deployment/speculative_execution" >}}). Nodes affected by either mechanism will not receive new allocations.

### Adaptive Scheduler
The [Adaptive Scheduler]({{< ref "docs/deployment/elastic_scaling" >}}) respects the Management Node Quarantine when making scaling decisions.

### High Availability
In HA setups, the quarantine state is managed by the active JobManager. When a new JobManager becomes active, the quarantine state is not automatically restored and must be reconfigured if needed.

## Troubleshooting

### Common Issues

**Node not getting quarantined**:
- Verify the node ID is correct
- Check that `cluster.management.node-quarantine.enabled` is set to `true`
- Ensure the REST API request is properly formatted

**Resource shortage after quarantine**:
- Check available resources with `GET /overview`
- Consider removing quarantine from some nodes or scaling up the cluster
- Review job resource requirements

**Quarantine not persisting**:
- The Management Node Quarantine state is not persisted across restarts
- Reconfigure quarantined nodes after JobManager restart if needed
- Consider implementing external automation for persistent quarantine

### Monitoring

Monitor the Management Node Quarantine through:
- REST API: `GET /cluster/node-quarantine`
- Flink Web UI: Cluster overview page (if UI support is available)
- Logs: Check JobManager logs for quarantine operations
- Metrics: Monitor cluster resource utilization after quarantining nodes

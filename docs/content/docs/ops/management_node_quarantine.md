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

The Management Node Quarantine is a feature that allows cluster administrators to temporarily quarantine problematic TaskManagers from receiving new task allocations. This is designed for scenarios where a TaskManager is experiencing issues — such as hardware degradation (e.g., disk errors, memory faults), elevated error rates, or network instability — and you want to stop scheduling new tasks on it while allowing existing tasks to finish gracefully.

By preventing new slot allocations on a quarantined TaskManager, operators can isolate the impact of a problematic node without triggering immediate task failures or job restarts. This is particularly useful in large clusters where individual node issues should not require full cluster intervention.

## Overview

The Management Node Quarantine provides a way to:
- **Quarantine TaskManagers**: Prevent new slots from being allocated on specific TaskManagers
- **Remove quarantine**: Allow previously quarantined TaskManagers to receive new allocations again
- **View quarantined TaskManagers**: List all currently quarantined TaskManagers with their reasons and expiration times
- **Automatic cleanup**: Quarantined TaskManagers are automatically released after a specified duration

Unlike the [speculative execution blocklist]({{< ref "docs/deployment/speculative_execution" >}}), the Management Node Quarantine is manually controlled by administrators and is intended for operational management rather than automatic performance optimization.

**Important**: Quarantining a TaskManager affects the entire process — all slots on that TaskManager will stop receiving new allocations. This means it is best suited for process-wide issues rather than individual slot or task-level problems.

## Configuration

The Management Node Quarantine can be configured using the following options:

| Configuration Key | Default | Description |
|-------------------|---------|-------------|
| `cluster.management.node-quarantine.enabled` | `false` | Whether to enable the management node quarantine feature |
| `cluster.management.node-quarantine.default-duration` | `10min` | Default duration for quarantining TaskManagers when no explicit duration is provided |
| `cluster.management.node-quarantine.max-duration` | `24h` | Maximum allowed duration for quarantining TaskManagers |

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

**Possible Errors**:
- `400 Bad Request`: Missing required fields (`nodeId` or `reason`), invalid duration format, or duration exceeds `max-duration`.
- `409 Conflict`: The node quarantine feature is not enabled (set `cluster.management.node-quarantine.enabled: true`).
- `500 Internal Server Error`: An unexpected error occurred while processing the request.

### Remove Node from Quarantine

**Endpoint**: `DELETE /cluster/node-quarantine/{nodeId}`

**Parameters**:
- `nodeId` (path parameter): The ID of the TaskManager to remove from quarantine

**Response**:
```json
{
  "status": "success",
  "message": "Node taskmanager-1 has been removed from the quarantine list"
}
```

**Possible Errors**:
- `404 Not Found`: The specified TaskManager is not in the quarantine list.
- `409 Conflict`: The node quarantine feature is not enabled.

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

### Typical Use Cases

1. **Problematic node isolation**: A TaskManager is producing elevated error rates or exhibiting degraded performance (e.g., slow disk I/O, memory pressure). Quarantine it so new tasks are not placed there, while existing tasks drain naturally.
2. **Pre-maintenance preparation**: Before performing rolling hardware or OS upgrades, quarantine the target TaskManager first, wait for running tasks to complete or reschedule, then proceed with maintenance.
3. **Incident investigation**: During an incident, quarantine a suspected TaskManager to prevent further impact while you investigate root cause.

### Using curl

```bash
# Quarantine a TaskManager experiencing disk errors for 2 hours
curl -X POST http://localhost:8081/cluster/node-quarantine \
  -H "Content-Type: application/json" \
  -d '{
    "nodeId": "taskmanager-1",
    "reason": "Disk I/O errors detected, investigating",
    "duration": "PT2H"
  }'

# List all quarantined TaskManagers
curl http://localhost:8081/cluster/node-quarantine

# Remove a TaskManager from quarantine after issue is resolved
curl -X DELETE http://localhost:8081/cluster/node-quarantine/taskmanager-1
```

## Behavior and Limitations

### Quarantine Behavior
- **New allocations**: Quarantined TaskManagers will not receive new slot allocations
- **Existing tasks**: Tasks already running on quarantined TaskManagers continue to run normally
- **Automatic expiration**: TaskManagers are automatically released from quarantine after the specified duration, once the periodic cleanup runs
- **Immediate effect**: Quarantine takes effect immediately after the API call
- **Expiration semantics**: A TaskManager does not become available the instant its expiration time passes. It becomes available only after the periodic cleanup scheduler removes it from the quarantine list. The cleanup runs every 30 seconds by default.

### Impact on Running Jobs
Quarantining a TaskManager can reduce the available parallelism for jobs. Potential consequences include:
- **Reduced throughput**: If fewer TaskManagers are available, tasks may be concentrated on remaining nodes, leading to higher per-node load.
- **Back pressure**: If remaining TaskManagers cannot handle the full workload, upstream operators may experience back pressure, which can reduce overall throughput.
- **SLA impact**: For latency-sensitive jobs, reduced resources may cause processing delays and potential SLA violations.
- **Scaling limitations**: The Adaptive Scheduler may not be able to scale jobs to their desired parallelism if too many TaskManagers are quarantined.

### Limitations
- **Cluster stability**: Quarantining too many TaskManagers may cause resource shortage
- **Running jobs**: Existing tasks on quarantined TaskManagers are not affected
- **Persistence**: Quarantine state is not persisted across JobManager restarts
- **Network partitions**: Quarantined TaskManagers that become unreachable are handled by Flink's failure detection

### Best Practices
1. **Gradual quarantine**: When quarantining multiple TaskManagers, do so one at a time and monitor resource utilization between each quarantine action. As a rule of thumb, avoid quarantining more than 10-20% of total TaskManagers simultaneously to maintain cluster stability.
2. **Monitor resources**: After quarantining a TaskManager, check the cluster overview (`GET /overview`) to verify that sufficient free slots remain. Ensure the number of available slots still meets the aggregate slot demand of all running and pending jobs.
3. **Clear reasons**: Provide clear, descriptive reasons for quarantining TaskManagers to aid in troubleshooting and audit.
4. **Reasonable durations**: Use appropriate quarantine durations based on the expected resolution time. For investigation, shorter durations (30 min – 1 hour) are recommended; for planned maintenance, match the expected maintenance window.
5. **Coordinate with job owners**: Before quarantining TaskManagers running critical jobs, notify job owners. They may experience slower throughput or potential SLA degradation. Consider scheduling quarantine during low-traffic periods when possible.

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
- Logs: Check JobManager logs for quarantine operations (search for `NodeQuarantine` in logs)
- Metrics: Monitor cluster resource utilization (`numSlotsAvailable`, `numSlotsTotal`) after quarantining TaskManagers to ensure sufficient capacity

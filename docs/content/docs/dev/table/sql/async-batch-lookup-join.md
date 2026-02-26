---
title: "Async Batch Lookup Join"
weight: 42
type: docs
aliases:
  - /dev/table/sql/async-batch-lookup-join.html
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

# Async Batch Lookup Join

Async Batch Lookup Join is a performance optimization for temporal table joins (dimension table lookups) in Flink streaming jobs. Instead of performing individual lookups for each record, this feature batches multiple lookup requests together, significantly reducing network overhead and improving throughput.

## Overview

Traditional temporal table joins process lookup requests one by one, which can create performance bottlenecks in high-throughput scenarios due to:
- High network round-trip overhead
- Inefficient resource utilization
- Increased latency under load

Async Batch Lookup Join addresses these issues by:
- **Batching**: Collecting multiple lookup requests into batches
- **Asynchronous Processing**: Non-blocking execution to maintain low latency
- **Configurable Flushing**: Automatic batch processing based on size or time triggers

## Configuration

### Enable Async Batch Lookup Join

```sql
SET 'table.optimizer.dim-lookup-join.batch.enabled' = 'true';
```

### Batch Size Configuration

Controls how many lookup requests are batched together:

```sql
SET 'table.optimizer.dim-lookup-join.batch.size' = '100';
```

**Default**: `100`
**Range**: `1` to `Integer.MAX_VALUE`

### Flush Interval Configuration

Controls the maximum time to wait before flushing a batch:

```sql
SET 'table.optimizer.dim-lookup-join.batch.flush.millis' = '2000';
```

**Default**: `2000` (2 seconds)
**Range**: `1` to `Long.MAX_VALUE`

## Usage Examples

### Basic Usage

```sql
-- Enable async batch lookup join
SET 'table.optimizer.dim-lookup-join.batch.enabled' = 'true';
SET 'table.optimizer.dim-lookup-join.batch.size' = '50';
SET 'table.optimizer.dim-lookup-join.batch.flush.millis' = '1000';

-- Create main stream table
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

-- Create dimension table
CREATE TABLE user_info (
    user_id BIGINT,
    user_name STRING,
    user_level STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/test',
    'table-name' = 'user_info'
);

-- Temporal table join (automatically uses batch mode when enabled)
SELECT 
    o.order_id,
    o.user_id,
    u.user_name,
    u.user_level,
    o.product_id,
    o.order_time
FROM orders o
JOIN user_info FOR SYSTEM_TIME AS OF o.order_time AS u
ON o.user_id = u.user_id;
```

### Performance Tuning

#### High Throughput Scenario
```sql
-- Optimize for high throughput
SET 'table.optimizer.dim-lookup-join.batch.enabled' = 'true';
SET 'table.optimizer.dim-lookup-join.batch.size' = '200';        -- Larger batch size
SET 'table.optimizer.dim-lookup-join.batch.flush.millis' = '500'; -- Shorter flush interval
```

#### Low Latency Scenario
```sql
-- Optimize for low latency
SET 'table.optimizer.dim-lookup-join.batch.enabled' = 'true';
SET 'table.optimizer.dim-lookup-join.batch.size' = '20';         -- Smaller batch size
SET 'table.optimizer.dim-lookup-join.batch.flush.millis' = '100'; -- Very short flush interval
```

## Performance Benefits

### Throughput Improvement
- **Reduced Network Overhead**: Batch requests minimize network round-trips
- **Better Resource Utilization**: More efficient use of database connections
- **Higher QPS**: Significantly increased queries per second

### Latency Optimization
- **Asynchronous Processing**: Non-blocking execution prevents pipeline stalls
- **Configurable Batching**: Balance between batch efficiency and latency requirements
- **Object Pool**: Reduced garbage collection overhead

## Best Practices

### Batch Size Tuning
- **Start with default**: Begin with the default batch size of 100
- **Monitor performance**: Observe throughput and latency metrics
- **Adjust gradually**: Increase batch size for higher throughput, decrease for lower latency

### Flush Interval Tuning
- **Consider data freshness**: Shorter intervals provide fresher data but may reduce batch efficiency
- **Balance with batch size**: Ensure most batches reach the target size before timeout
- **Monitor batch utilization**: Aim for high batch fill rates

### Monitoring
Track these key metrics:
- Lookup QPS (queries per second)
- Average lookup latency
- Batch utilization rate
- Memory usage

## Limitations and Considerations

### Memory Usage
- Batching requires additional memory to buffer requests
- Memory usage scales with batch size and parallelism
- Monitor memory consumption in production

### Data Freshness
- Batch processing introduces slight delays
- Flush interval affects data freshness
- Consider requirements for real-time data

### Compatibility
- Requires dimension tables that support the lookup interface
- Some connectors may not benefit from batching
- Test thoroughly with your specific setup

## Troubleshooting

### Common Issues

**High Memory Usage**
- Reduce batch size
- Decrease flush interval
- Check for memory leaks in custom connectors

**Increased Latency**
- Reduce batch size
- Decrease flush interval
- Verify network connectivity to dimension tables

**Low Batch Utilization**
- Increase flush interval
- Check input data rate
- Verify batch size configuration

### Debug Configuration

Enable debug logging to monitor batch behavior:

```sql
-- Enable detailed logging (for development/testing only)
SET 'table.optimizer.dim-lookup-join.batch.enabled' = 'true';
-- Monitor logs for batch statistics
```

## Migration Guide

### From Single Lookup Join

1. **Enable gradually**: Start with a small batch size
2. **Monitor performance**: Compare metrics before and after
3. **Tune parameters**: Adjust based on observed performance
4. **Test thoroughly**: Validate correctness and performance in staging

### Rollback Plan

To disable async batch lookup join:

```sql
SET 'table.optimizer.dim-lookup-join.batch.enabled' = 'false';
```

The system will automatically fall back to single lookup join mode.
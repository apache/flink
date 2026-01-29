# Flink Triton Model Integration

## ⚠️ Experimental / MVP Status

**This module is currently experimental and designed for batch-oriented inference workloads.**

### Scope and Positioning

- **Primary Use Case**: Batch inference via `ML_PREDICT` on bounded tables
- **Stability**: Experimental - APIs may evolve in future releases
- **Target Scenarios**: Offline processing, batch scoring, model evaluation pipelines

### Non-Goals (for v1)

- **Streaming inference**: Real-time/low-latency async inference in streaming jobs (future scope)
- **Multi-input/output models**: Complex tensor schemas (ROW, MAP types - planned for v2+)
- **gRPC protocol**: Binary protocol support (HTTP/REST only in v1)

### Why Batch-First?

**Important**: The term "batch-first" refers to this module's **primary use case** (batch table processing via `ML_PREDICT`), NOT to request-level batching semantics.

**Current Request Model (v1):**
- Each Flink record triggers **ONE HTTP inference request** (1:1 mapping)
- No Flink-side mini-batch aggregation in this version
- Batching efficiency comes from:
  - **Triton server-side dynamic batching**: Configure in model's `config.pbtxt` to aggregate concurrent requests
  - **Flink table-level parallelism**: Natural concurrency from parallel source reads
  - **AsyncDataStream capacity**: Buffer size controls concurrent in-flight requests

This initial version focuses on correctness and API compatibility with Triton's inference protocol. The batch-oriented **use case** allows us to:
- Validate type mappings and schema handling with simpler control flow
- Establish stable configuration patterns before adding streaming complexity
- Gather community feedback on API design before committing to streaming semantics

**Future Enhancement (v2+):** Flink-side mini-batch buffer (N rows / T milliseconds) to reduce HTTP overhead for high-throughput scenarios.

**For streaming use cases**, consider evaluating this module after v2 when async streaming patterns are stabilized.

---

This module provides integration between Apache Flink and NVIDIA Triton Inference Server, enabling model inference within Flink batch applications.

## Features

- **REST API Integration**: Communicates with Triton Inference Server via HTTP/REST API
- **Batch Inference Support**: Designed for `ML_PREDICT` in batch table queries
- **Flexible Configuration**: Comprehensive configuration options for various use cases
- **Multi-Type Support**: Supports various input/output data types (STRING, INT, FLOAT, DOUBLE, ARRAY, etc.)
- **Error Handling**: Built-in retry mechanisms and error handling
- **Resource Management**: Efficient HTTP client pooling and resource management

## Configuration Options

### Required Options

| Option | Type | Description |
|--------|------|-------------|
| `endpoint` | String | Base URL of the Triton Inference Server (e.g., `http://localhost:8000` or `http://localhost:8000/v2/models`). The integration will auto-complete to the full inference path. |
| `model-name` | String | Name of the model to invoke on Triton server |
| `model-version` | String | Version of the model to use (defaults to "latest") |

### Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `timeout` | Long | 30000 | HTTP request timeout in milliseconds (connect + read + write). |
| `max-retries` | Integer | 3 | Maximum retry attempts for connection failures (IOException). HTTP 4xx/5xx errors are NOT retried automatically. |
| `batch-size` | Integer | 1 | **Reserved for future use (v2+).** Currently has NO effect in v1 - each Flink record triggers one HTTP request. Future versions will support Flink-side mini-batch aggregation (buffer N records or T milliseconds). For batching efficiency in v1, configure Triton's `dynamic_batching` in model config and tune AsyncDataStream capacity. |
| `priority` | Integer | - | Request priority level (0-255, higher values = higher priority). *Triton-specific: See Triton docs for server support.* |
| `sequence-id` | String | - | Sequence ID for stateful models. *Triton-specific: For models with sequence/state handling.* |
| `sequence-start` | Boolean | false | Whether this is the start of a sequence for stateful models. *Triton-specific.* |
| `sequence-end` | Boolean | false | Whether this is the end of a sequence for stateful models. *Triton-specific.* |
| `binary-data` | Boolean | false | Whether to use binary data transfer. **Not implemented in v1** (reserved for future use, currently JSON-only). |
| `compression` | String | - | Compression algorithm to use (e.g., 'gzip') |
| `auth-token` | String | - | Authentication token for secured Triton servers |
| `custom-headers` | String | - | Custom HTTP headers in JSON format |
| `flatten-batch-dim` | Boolean | false | *Advanced/Triton-specific*: Remove leading batch dimension from input shape. Use when Triton model expects `[N]` but Flink provides `[1,N]`. |

### Important Notes on Batching

**Current v1 Behavior**: Each Flink record triggers **one HTTP request** (1:1 mapping). There is no Flink-side batching in the initial version.

- The `batch-size` option is reserved for future use (Flink-side request aggregation)
- For server-side batching: Configure Triton's model `config.pbtxt` with `dynamic_batching` settings
- Batch inference workloads naturally benefit from table-level parallelism without explicit batching

**Future enhancement** (v2+): Flink-side batching to reduce HTTP overhead for high-throughput scenarios.

## Usage Example

### Basic Text Processing

```sql
CREATE MODEL my_triton_model (
  input STRING,
  output STRING
) WITH (
  'provider' = 'triton',
  'endpoint' = 'http://localhost:8000/v2/models',
  'model-name' = 'text-classification',
  'model-version' = '1',
  'timeout' = '10000',
  'max-retries' = '5'
);
```

### Image Classification with Array Input

```sql
CREATE MODEL image_classifier (
  image_data ARRAY<FLOAT>,
  predictions ARRAY<FLOAT>
) WITH (
  'provider' = 'triton',
  'endpoint' = 'http://localhost:8000/v2/models',
  'model-name' = 'resnet50',
  'model-version' = '1'
);
```

### Numeric Prediction

```sql
CREATE MODEL numeric_model (
  features ARRAY<DOUBLE>,
  score FLOAT
) WITH (
  'provider' = 'triton',
  'endpoint' = 'http://localhost:8000/v2/models',
  'model-name' = 'linear-regression',
  'model-version' = 'latest'
);
```

### Table API

```java
// Create table environment
TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

// Register the model
tableEnv.executeSql(
    "CREATE MODEL my_triton_model (" +
    "  input STRING," +
    "  output STRING" +
    ") WITH (" +
    "  'provider' = 'triton'," +
    "  'endpoint' = 'http://localhost:8000/v2/models'," +
    "  'model-name' = 'text-classification'," +
    "  'model-version' = '1'" +
    ")"
);

// Use the model for inference
Table result = tableEnv.sqlQuery(
    "SELECT input, ML_PREDICT('my_triton_model', input) as prediction " +
    "FROM input_table"
);
```

## Supported Data Types

**Current Version (v1) Limitation**: Only single input column and single output column are supported per model.

The Triton integration supports the following Flink data types:

| Flink Type | Triton Type | Description |
|------------|-------------|-------------|
| `BOOLEAN` | `BOOL` | Boolean values |
| `TINYINT` | `INT8` | 8-bit signed integer |
| `SMALLINT` | `INT16` | 16-bit signed integer |
| `INT` | `INT32` | 32-bit signed integer |
| `BIGINT` | `INT64` | 64-bit signed integer |
| `FLOAT` | `FP32` | 32-bit floating point |
| `DOUBLE` | `FP64` | 64-bit floating point |
| `STRING` / `VARCHAR` | `BYTES` | String/text data |
| `ARRAY<T>` | `TYPE[]` | Array of any supported type |

### Multi-Tensor Models (Workarounds for v1)

If your Triton model requires multiple input tensors, consider these approaches:

1. **JSON Encoding**: Serialize multiple fields into a JSON STRING
2. **Array Packing**: Concatenate values into a single ARRAY<T>
3. **Future Support**: ROW<...> and MAP<...> types are planned for future releases

### Type Mapping Examples

```sql
-- String input/output (text processing)
CREATE MODEL text_model (
  text STRING,
  result STRING
) WITH ('provider' = 'triton', ...);

-- Array input/output (image processing, embeddings)
CREATE MODEL embedding_model (
  text STRING,
  embedding ARRAY<FLOAT>
) WITH ('provider' = 'triton', ...);

-- Numeric computation
CREATE MODEL regression_model (
  features ARRAY<DOUBLE>,
  prediction DOUBLE
) WITH ('provider' = 'triton', ...);
```

## Advanced Configuration

```sql
CREATE MODEL advanced_triton_model (
  input STRING,
  output STRING
) WITH (
  'provider' = 'triton',
  'endpoint' = 'https://triton.example.com/v2/models',
  'model-name' = 'advanced-nlp-model',
  'model-version' = 'latest',
  'timeout' = '15000',
  'max-retries' = '3',
  'batch-size' = '4',
  'priority' = '100',
  'auth-token' = 'your-auth-token-here',
  'custom-headers' = '{"X-Custom-Header": "custom-value"}',
  'compression' = 'gzip'
);
```

## Triton Server Setup

To use this integration, you need a running Triton Inference Server. Here's a basic setup:

### Using Docker

```bash
# Pull Triton server image
docker pull nvcr.io/nvidia/tritonserver:23.10-py3

# Run Triton server with your model repository
docker run --rm -p 8000:8000 -p 8001:8001 -p 8002:8002 \
  -v /path/to/your/model/repository:/models \
  nvcr.io/nvidia/tritonserver:23.10-py3 \
  tritonserver --model-repository=/models
```

### Model Repository Structure

```
model_repository/
├── text-classification/
│   ├── config.pbtxt
│   └── 1/
│       └── model.py  # or model.onnx, model.plan, etc.
└── other-model/
    ├── config.pbtxt
    └── 1/
        └── model.savedmodel/
```

## Error Handling

The integration includes comprehensive error handling:

- **Connection Errors**: Automatic retry with exponential backoff (OkHttp built-in)
- **Timeout Handling**: Configurable HTTP request timeout (default 30s)
- **HTTP Errors**: 4xx/5xx responses are NOT automatically retried
  - 400 Bad Request: Usually indicates shape/type mismatch
  - 404 Not Found: Model or version not available
  - 500 Internal Server Error: Triton inference failure
- **Serialization Errors**: JSON parsing and type validation errors

### Retry Behavior Matrix

| Error Type | Trigger | Flink Behavior | Triton Behavior |
|------------|---------|----------------|-----------------|
| Connection Timeout | Network issue | Fails async operation | N/A |
| HTTP Timeout | Slow inference | Fails after `timeout` ms | N/A |
| Connection Failure (IOException) | Network error | Retries up to `max-retries` | N/A |
| HTTP 4xx | Client error (bad input/shape) | No retry, fails immediately | Returns error JSON |
| HTTP 5xx | Server error (inference crash) | No retry, fails immediately | Returns error JSON |
| JSON Parse Error | Invalid response | No retry, fails immediately | N/A |

**Important**: Configure Flink's async timeout separately from HTTP timeout to avoid cascading failures:
```java
// Flink async timeout should be > HTTP timeout + retry overhead
AsyncDataStream.unorderedWait(stream, asyncFunc, 60000, TimeUnit.MILLISECONDS);
```

## Performance Considerations

- **Connection Pooling**: HTTP clients are shared across function instances with the same timeout/retry configuration (reference-counted singleton per JVM)
- **Asynchronous Processing**: Non-blocking requests prevent thread starvation
- **Batch Processing**: 
  - **Triton-side**: Enable dynamic batching in Triton's model config for optimal throughput
  - **Flink-side**: Configure AsyncDataStream capacity for concurrent request buffering
- **Resource Management**: Automatic cleanup of HTTP resources via reference counting

### Performance Tuning Tips

1. **Increase Async Capacity**: For high-throughput scenarios
   ```java
   AsyncDataStream.unorderedWait(stream, asyncFunc, timeout, TimeUnit.MILLISECONDS, 200); // capacity=200
   ```

2. **Enable Triton Dynamic Batching**: In model's `config.pbtxt`
   ```
   dynamic_batching {
     preferred_batch_size: [ 4, 8, 16 ]
     max_queue_delay_microseconds: 100
   }
   ```

3. **Tune Parallelism**: Match Flink parallelism to Triton server capacity
   ```java
   dataStream.map(...).setParallelism(10); // Adjust based on server resources
   ```

## Monitoring and Debugging

Enable debug logging to monitor the integration:

```properties
# In log4j2.properties
logger.triton.name = org.apache.flink.model.triton
logger.triton.level = DEBUG
```

This will provide detailed logs about:
- HTTP request/response details
- Client connection management
- Error conditions and retries
- Performance metrics

## Dependencies

This module includes the following key dependencies:
- OkHttp for HTTP client functionality (with connection pooling)
- Jackson for JSON processing
- Flink Table API for model integration

All dependencies are shaded to avoid conflicts with your application.

## Limitations and Future Work

### Current Limitations (v1)

1. **Single Input/Output Only**: Each model must have exactly one input column and one output column
2. **REST API Only**: Uses HTTP/REST protocol. gRPC is not yet supported
3. **No Flink-Side Batching**: Each record triggers a separate HTTP request (relies on Triton's server-side batching)
4. **Binary Data Mode**: Declared but not fully implemented (JSON only)

### Testing and Validation

**Unit Test Coverage**: This module includes comprehensive unit tests for:
- Type mapping logic (`TritonTypeMapper`)
- HTTP request/response formatting
- Configuration validation
- Provider factory registration

**Integration Testing**: End-to-end tests with a live Triton server are **not included** in this PR due to:
- CI environment constraints (no GPU/Triton infrastructure in Flink CI)
- Complexity of Docker-in-Docker setup for model serving
- Focus on **protocol correctness** rather than end-to-end deployment validation

**Manual Validation**: The module has been manually tested with local Triton instances across various model types (text classification, embeddings, numeric regression). Users are encouraged to validate with their specific Triton deployments.

**Note**: This testing approach is consistent with other `flink-models` providers (e.g., `flink-model-openai` tests protocol compliance without live API calls).

### Planned Enhancements (v2+)

- **Multi-Input/Output Support**: Using ROW<...> or MAP<...> types to map multiple Triton tensors
- **gRPC Protocol**: Native gRPC support for improved performance and streaming
- **Flink-Side Batching**: Optional aggregation of multiple records before sending to Triton
- **Binary Data Transfer**: Efficient binary serialization for large tensor data

**Feedback Welcome**: Please share your use cases and requirements via JIRA or mailing lists to help prioritize these features.

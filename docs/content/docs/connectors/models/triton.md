---
title: "Triton"
weight: 2
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

# Triton

## Overview

The Triton Model Function allows Flink SQL to call [NVIDIA Triton Inference Server](https://github.com/triton-inference-server/server) for real-time model inference tasks. Triton Inference Server is a high-performance inference serving solution that supports multiple machine learning frameworks including TensorFlow, PyTorch, ONNX, TensorRT, and more.

Key features:
* **High Performance**: Optimized for low-latency and high-throughput inference
* **Multi-Framework Support**: Works with models from various ML frameworks
* **Asynchronous Processing**: Non-blocking inference requests for better resource utilization
* **Flexible Configuration**: Comprehensive configuration options for different use cases
* **Resource Management**: Efficient HTTP client pooling and automatic resource cleanup
* **Fault Tolerance**: Built-in retry mechanism with configurable attempts

{{< hint info >}}
The `flink-model-triton` module is available since Flink 2.0. Ensure you have access to a running Triton Inference Server instance.
{{< /hint >}}

## Usage Examples

### Example 1: Text Classification (Basic)

This example demonstrates sentiment analysis on movie reviews:

{{< tabs "text-classification" >}}
{{< tab "SQL" >}}
```sql
-- Create the Triton model
CREATE MODEL triton_sentiment_classifier
INPUT (`input` STRING)
OUTPUT (`output` STRING)
WITH (
    'provider' = 'triton',
    'endpoint' = 'http://localhost:8000/v2/models',
    'model-name' = 'text-classification',
    'model-version' = '1',
    'timeout' = '10000',
    'max-retries' = '3'
);

-- Prepare source data
CREATE TEMPORARY VIEW movie_reviews(id, movie_name, user_review, actual_sentiment)
AS VALUES
  (1, 'Great Movie', 'This movie was absolutely fantastic! Great acting and storyline.', 'positive'),
  (2, 'Boring Film', 'I fell asleep halfway through. Very disappointing.', 'negative'),
  (3, 'Average Show', 'It was okay, nothing special but not terrible either.', 'neutral');

-- Create output table
CREATE TEMPORARY TABLE classified_reviews(
  id BIGINT,
  movie_name VARCHAR,
  predicted_sentiment VARCHAR,
  actual_sentiment VARCHAR
) WITH (
  'connector' = 'print'
);

-- Classify sentiment
INSERT INTO classified_reviews
SELECT id, movie_name, output as predicted_sentiment, actual_sentiment
FROM ML_PREDICT(
  TABLE movie_reviews,
  MODEL triton_sentiment_classifier,
  DESCRIPTOR(user_review)
);
```
{{< /tab >}}
{{< tab "Table API (Java)" >}}
```java
TableEnvironment tEnv = TableEnvironment.create(...);

// Register the model
tEnv.executeSql(
    "CREATE MODEL triton_sentiment_classifier " +
    "INPUT (`input` STRING) " +
    "OUTPUT (`output` STRING) " +
    "WITH (" +
    "  'provider' = 'triton', " +
    "  'endpoint' = 'http://localhost:8000/v2/models', " +
    "  'model-name' = 'text-classification', " +
    "  'model-version' = '1', " +
    "  'timeout' = '10000', " +
    "  'max-retries' = '3'" +
    ")"
);

// Register source table
tEnv.executeSql(
    "CREATE TEMPORARY VIEW movie_reviews(id, movie_name, user_review, actual_sentiment) " +
    "AS VALUES " +
    "  (1, 'Great Movie', 'This movie was absolutely fantastic!', 'positive'), " +
    "  (2, 'Boring Film', 'I fell asleep halfway through.', 'negative')"
);

// Perform classification
Table result = tEnv.sqlQuery(
    "SELECT id, movie_name, output as predicted_sentiment " +
    "FROM ML_PREDICT(" +
    "  TABLE movie_reviews, " +
    "  MODEL triton_sentiment_classifier, " +
    "  DESCRIPTOR(user_review)" +
    ")"
);

result.execute().print();
```
{{< /tab >}}
{{< /tabs >}}

### Example 2: Image Classification with Streaming

Classify images from a Kafka stream using a ResNet model:

```sql
-- Register image classification model
CREATE MODEL image_classifier
INPUT (image_pixels ARRAY<FLOAT>)
OUTPUT (predicted_class STRING, confidence FLOAT)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'resnet50',
  'model-version' = '1',
  'timeout' = '10000',
  'compression' = 'gzip'  -- Enable compression for large image data
);

-- Source table from Kafka
CREATE TEMPORARY TABLE image_stream (
  image_id STRING,
  image_pixels ARRAY<FLOAT>,  -- Preprocessed image as float array
  upload_time TIMESTAMP(3),
  WATERMARK FOR upload_time AS upload_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'images',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- Classify images
SELECT 
  image_id,
  predicted_class,
  confidence,
  upload_time
FROM ML_PREDICT(
  TABLE image_stream,
  MODEL image_classifier,
  DESCRIPTOR(image_pixels)
);
```

### Example 3: Real-time Fraud Detection

High-priority inference for fraud detection:

```sql
-- Create fraud detection model with high priority
CREATE MODEL fraud_detector
INPUT (
  user_id BIGINT,
  amount DOUBLE,
  merchant_id STRING,
  device_fingerprint STRING
)
OUTPUT (fraud_score FLOAT)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'fraud_detection_model',
  'timeout' = '5000',
  'priority' = '200',  -- High priority for critical transactions
  'max-retries' = '5'
);

CREATE TEMPORARY TABLE transactions (
  transaction_id STRING,
  user_id BIGINT,
  amount DECIMAL(10, 2),
  merchant_id STRING,
  device_fingerprint STRING,
  transaction_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- Flag suspicious transactions
SELECT 
  transaction_id,
  user_id,
  amount,
  fraud_score,
  CASE 
    WHEN fraud_score > 0.8 THEN 'HIGH_RISK'
    WHEN fraud_score > 0.5 THEN 'MEDIUM_RISK'
    ELSE 'LOW_RISK'
  END AS risk_level
FROM ML_PREDICT(
  TABLE transactions,
  MODEL fraud_detector,
  DESCRIPTOR(user_id, amount, merchant_id, device_fingerprint)
)
WHERE fraud_score > 0.5;  -- Alert on suspicious transactions
```

### Example 4: Recommendation System

Product recommendations based on user behavior:

```sql
-- Register recommendation model
CREATE MODEL recommender
INPUT (
  user_features ARRAY<FLOAT>,
  browsing_history ARRAY<STRING>,
  context_features ARRAY<FLOAT>
)
OUTPUT (recommended_products ARRAY<STRING>, scores ARRAY<FLOAT>)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'product_recommender',
  'model-version' = '2',
  'batch-size' = '4'  -- Batch multiple requests
);

CREATE TEMPORARY TABLE user_activity (
  user_id BIGINT,
  user_features ARRAY<FLOAT>,
  browsing_history ARRAY<STRING>,
  context_features ARRAY<FLOAT>,
  event_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_events',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- Generate personalized recommendations
SELECT 
  user_id,
  recommended_products,
  scores,
  event_time
FROM ML_PREDICT(
  TABLE user_activity,
  MODEL recommender,
  DESCRIPTOR(user_features, browsing_history, context_features)
);
```

### Example 5: Named Entity Recognition (NER)

Extract entities from text documents:

```sql
-- Register NER model with compression for large documents
CREATE MODEL ner_model
INPUT (document_text STRING)
OUTPUT (entities ARRAY<STRING>, entity_types ARRAY<STRING>)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'bert_ner',
  'compression' = 'gzip'
);

CREATE TEMPORARY TABLE documents (
  doc_id STRING,
  document_text STRING,
  source STRING,
  created_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'documents',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- Extract named entities
SELECT 
  doc_id,
  entities,
  entity_types,
  source
FROM ML_PREDICT(
  TABLE documents,
  MODEL ner_model,
  DESCRIPTOR(document_text)
);
```

### Example 6: Stateful Sequence Model

Use stateful models (RNN/LSTM) with sequence tracking:

```sql
-- Register stateful conversation model
CREATE MODEL conversation_model
INPUT (message_text STRING)
OUTPUT (bot_response STRING)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'chatbot_lstm',
  'sequence-id' = 'conv-001',  -- Unique sequence ID
  'sequence-start' = 'true',
  'sequence-end' = 'false'
);

CREATE TEMPORARY TABLE chat_messages (
  message_id STRING,
  user_id BIGINT,
  message_text STRING,
  timestamp TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'chat',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- Process conversation with context
SELECT 
  message_id,
  user_id,
  bot_response,
  timestamp
FROM ML_PREDICT(
  TABLE chat_messages,
  MODEL conversation_model,
  DESCRIPTOR(message_text)
);
```

### Example 7: Batch Inference

Perform batch inference on historical data:

```sql
-- Register model for batch processing
CREATE MODEL batch_classifier
INPUT (features ARRAY<DOUBLE>)
OUTPUT (prediction STRING, confidence DOUBLE)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'classifier',
  'timeout' = '60000',
  'batch-size' = '32'  -- Large batch for efficiency
);

-- Batch source table
CREATE TEMPORARY TABLE historical_data (
  id BIGINT,
  features ARRAY<DOUBLE>
) WITH (
  'connector' = 'filesystem',
  'path' = 'hdfs:///data/historical',
  'format' = 'parquet'
);

-- Batch inference with results written to sink
CREATE TEMPORARY TABLE classification_results (
  id BIGINT,
  prediction STRING,
  confidence DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = 'hdfs:///results/classifications',
  'format' = 'parquet'
);

INSERT INTO classification_results
SELECT id, prediction, confidence
FROM ML_PREDICT(
  TABLE historical_data,
  MODEL batch_classifier,
  DESCRIPTOR(features)
);
```

### Example 8: Secured Triton Server

Access a secured Triton server with authentication:

```sql
-- Register model with authentication
CREATE MODEL secure_model
INPUT (data STRING)
OUTPUT (result STRING)
WITH (
  'provider' = 'triton',
  'endpoint' = 'https://secure-triton:8000/v2/models',
  'model-name' = 'private_model',
  'auth-token' = 'Bearer your-token-here',
  'custom-headers' = '{"X-API-Key": "your-api-key", "X-Client-ID": "flink-job-123"}'
);

SELECT id, result
FROM ML_PREDICT(
  TABLE sensitive_data,
  MODEL secure_model,
  DESCRIPTOR(data)
);
```

{{< hint warning >}}
Never hardcode sensitive tokens in SQL. Use Flink's secret management or environment variables.
{{< /hint >}}

### Example 9: Array Type with Flatten Batch Dimension

For models that accept array inputs without batch dimension:

```sql
-- Create model with array input and flatten batch dimension
CREATE MODEL triton_vector_model
INPUT (input_vector ARRAY<FLOAT>)
OUTPUT (output_vector ARRAY<FLOAT>)
WITH (
    'provider' = 'triton',
    'endpoint' = 'http://localhost:8000/v2/models',
    'model-name' = 'vector-transform',
    'model-version' = '1',
    'flatten-batch-dim' = 'true'  -- Flatten [1,N] to [N]
);

-- Use the model for inference
CREATE TEMPORARY TABLE vector_input (
    id BIGINT,
    features ARRAY<FLOAT>
) WITH (
    'connector' = 'datagen',
    'fields.features.length' = '128'  -- 128-dimensional vector
);

SELECT id, output_vector
FROM ML_PREDICT(
  TABLE vector_input,
  MODEL triton_vector_model,
  DESCRIPTOR(features)
);
```

### Example 10: Advanced Configuration

For production environments with comprehensive settings:

```sql
CREATE MODEL triton_advanced_model
INPUT (`input` STRING)
OUTPUT (`output` STRING)
WITH (
    'provider' = 'triton',
    'endpoint' = 'https://triton.example.com/v2/models',
    'model-name' = 'advanced-nlp-model',
    'model-version' = 'latest',
    'timeout' = '15000',
    'max-retries' = '5',
    'batch-size' = '4',
    'priority' = '100',
    'auth-token' = 'Bearer your-auth-token-here',
    'custom-headers' = '{"X-Custom-Header": "custom-value", "X-Request-ID": "req-123"}',
    'compression' = 'gzip',
    'binary-data' = 'false'
);
```

## Model Options

### Required Options

{{< generated/triton_common_section >}}

### Optional Options

{{< generated/triton_advanced_section >}}

## Schema Requirement

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-center">Input Type</th>
            <th class="text-center">Output Type</th>
            <th class="text-left">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>BOOLEAN, TINYINT, SMALLINT, INT, BIGINT</td>
            <td>BOOLEAN, TINYINT, SMALLINT, INT, BIGINT</td>
            <td>Integer type inference</td>
        </tr>
        <tr>
            <td>FLOAT, DOUBLE</td>
            <td>FLOAT, DOUBLE</td>
            <td>Floating-point type inference</td>
        </tr>
        <tr>
            <td>STRING</td>
            <td>STRING</td>
            <td>Text-to-text inference (classification, generation, etc.)</td>
        </tr>
        <tr>
            <td>ARRAY&lt;numeric types&gt;</td>
            <td>ARRAY&lt;numeric types&gt;</td>
            <td>Array inference (vectors, tensors, etc.). Supports arrays of numeric types.</td>
        </tr>
    </tbody>
</table>

**Note**: Input and output types must match the types defined in your Triton model configuration.

To verify your model's expected input/output types, query the Triton server:
```bash
curl http://triton-server:8000/v2/models/{model_name}/config
```

## Triton Server Setup

To use this integration, you need a running Triton Inference Server. Here's a basic setup guide:

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

Your model repository should follow this structure:

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

### Example Model Configuration

Here's an example `config.pbtxt` for a text classification model:

```protobuf
name: "text-classification"
platform: "python"
max_batch_size: 8
input [
  {
    name: "INPUT_TEXT"
    data_type: TYPE_STRING
    dims: [ 1 ]
  }
]
output [
  {
    name: "OUTPUT_TEXT"
    data_type: TYPE_STRING
    dims: [ 1 ]
  }
]
```

## Performance Considerations

1. **Connection Pooling**: HTTP clients are pooled and reused for efficiency
2. **Asynchronous Processing**: Non-blocking requests prevent thread starvation
3. **Batch Processing**: Configure batch size for optimal throughput
   - Simple models: batch-size 1-4
   - Medium models: batch-size 4-16
   - Complex models: batch-size 16-32
4. **Resource Management**: Automatic cleanup of HTTP resources
5. **Timeout Configuration**: Set appropriate timeout values based on model complexity
   - Simple models: 1-5 seconds
   - Medium models (e.g., BERT): 5-30 seconds
   - Complex models (e.g., GPT): 30-120 seconds
6. **Retry Strategy**: Configure retry attempts for handling transient failures
7. **Compression**: Enable gzip compression for payloads > 1KB
8. **Parallelism**: Match Flink parallelism to Triton server capacity

## Best Practices

### Model Version Management

Pin model versions in production to ensure consistency:
```sql
'model-version' = '3'  -- Pin to version 3 instead of 'latest'
```

### Error Handling

Use default values on failure:
```sql
SELECT COALESCE(output, 'UNKNOWN') AS prediction
FROM ML_PREDICT(...)
```

### Resource Configuration

Configure sufficient memory and network buffers for high-throughput scenarios:
```yaml
taskmanager.memory.managed.size: 2gb
taskmanager.network.memory.fraction: 0.2
```

## Error Handling

The integration includes comprehensive error handling:

- **Connection Errors**: Automatic retry with exponential backoff
- **Timeout Handling**: Configurable request timeouts
- **HTTP Errors**: Detailed error messages from Triton server
- **Serialization Errors**: JSON parsing and validation errors

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

## Troubleshooting

### Connection Issues
- Verify Triton server is running: `curl http://triton-server:8000/v2/health/ready`
- Check network connectivity and firewall rules
- Ensure endpoint URL includes the correct protocol (http/https)

### Timeout Errors
- Increase timeout value: `'timeout' = '60000'`
- Check Triton server resource usage (CPU/GPU)
- Monitor Triton server logs for slow model execution

### Type Mismatch
- Verify model schema: `curl http://triton-server:8000/v2/models/{model}/config`
- Cast Flink types explicitly: `CAST(value AS FLOAT)`
- Ensure array dimensions match model expectations

### High Latency
- Enable request compression: `'compression' = 'gzip'`
- Increase Triton server instances
- Use dynamic batching in Triton server configuration
- Check network latency between Flink and Triton

## Dependencies

To use the Triton model function, you need to include the following dependency in your Flink application:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-model-triton</artifactId>
    <version>${flink.version}</version>
</dependency>
```

## Further Information

- [Triton Inference Server Documentation](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/)
- [Triton Model Configuration](https://github.com/triton-inference-server/server/blob/main/docs/user_guide/model_configuration.md)
- [Flink Async I/O]({{< ref "docs/dev/datastream/operators/asyncio" >}})
- [Flink Metrics]({{< ref "docs/ops/metrics" >}})

{{< top >}}
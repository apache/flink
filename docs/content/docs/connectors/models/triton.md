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

The Triton Model Function allows Flink SQL to call [NVIDIA Triton Inference Server](https://github.com/triton-inference-server/server) for real-time model inference tasks.

## Overview

The function supports calling remote Triton Inference Server via Flink SQL for prediction/inference tasks. Triton Inference Server is a high-performance inference serving solution that supports multiple machine learning frameworks including TensorFlow, PyTorch, ONNX, and more.

Key features:
* **High Performance**: Optimized for low-latency and high-throughput inference
* **Multi-Framework Support**: Works with models from various ML frameworks
* **Asynchronous Processing**: Non-blocking inference requests for better resource utilization
* **Flexible Configuration**: Comprehensive configuration options for different use cases
* **Resource Management**: Efficient HTTP client pooling and automatic resource cleanup

## Usage Examples

The following example creates a Triton model for text classification and uses it to analyze sentiment in movie reviews.

First, create the Triton model with the following SQL statement:

```sql
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
```

Suppose the following data is stored in a table named `movie_reviews`, and the prediction result is to be stored in a table named `classified_reviews`:

```sql
CREATE TEMPORARY VIEW movie_reviews(id, movie_name, user_review, actual_sentiment)
AS VALUES
  (1, 'Great Movie', 'This movie was absolutely fantastic! Great acting and storyline.', 'positive'),
  (2, 'Boring Film', 'I fell asleep halfway through. Very disappointing.', 'negative'),
  (3, 'Average Show', 'It was okay, nothing special but not terrible either.', 'neutral');

CREATE TEMPORARY TABLE classified_reviews(
  id BIGINT,
  movie_name VARCHAR,
  predicted_sentiment VARCHAR,
  actual_sentiment VARCHAR
) WITH (
  'connector' = 'print'
);
```

Then the following SQL statement can be used to classify sentiment for movie reviews:

```sql
INSERT INTO classified_reviews
SELECT id, movie_name, output as predicted_sentiment, actual_sentiment
FROM ML_PREDICT(
  TABLE movie_reviews,
  MODEL triton_sentiment_classifier,
  DESCRIPTOR(user_review)
);
```

### Advanced Configuration Example

For production environments with authentication and custom headers:

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
    'compression' = 'gzip'
);
```

### Array Type Inference Example

For models that accept array inputs (e.g., vector embeddings, image features):

```sql
-- Create model with array input
CREATE MODEL triton_vector_model
INPUT (input_vector ARRAY<FLOAT>)
OUTPUT (output_vector ARRAY<FLOAT>)
WITH (
    'provider' = 'triton',
    'endpoint' = 'http://localhost:8000/v2/models',
    'model-name' = 'vector-transform',
    'model-version' = '1',
    'flatten-batch-dim' = 'true'  -- If model doesn't expect batch dimension
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

### Stateful Model Example

For stateful models that require sequence processing:

```sql
CREATE MODEL triton_sequence_model
INPUT (`input` STRING)
OUTPUT (`output` STRING)
WITH (
    'provider' = 'triton',
    'endpoint' = 'http://localhost:8000/v2/models',
    'model-name' = 'sequence-model',
    'model-version' = '1',
    'sequence-id' = 'seq-001',
    'sequence-start' = 'true',
    'sequence-end' = 'false'
);
```

## Model Options

### Required Options

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Option</th>
            <th class="text-center" style="width: 8%">Required</th>
            <th class="text-center" style="width: 7%">Default</th>
            <th class="text-center" style="width: 10%">Type</th>
            <th class="text-center" style="width: 50%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <h5>provider</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Specifies the model function provider to use, must be 'triton'.</td>
        </tr>
        <tr>
            <td>
                <h5>endpoint</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Full URL of the Triton Inference Server endpoint, e.g. <code>http://localhost:8000/v2/models</code>.</td>
        </tr>
        <tr>
            <td>
                <h5>model-name</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Name of the model to invoke on Triton server.</td>
        </tr>
        <tr>
            <td>
                <h5>model-version</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">latest</td>
            <td>String</td>
            <td>Version of the model to use. Defaults to 'latest'.</td>
        </tr>
    </tbody>
</table>

### Optional Options

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Option</th>
            <th class="text-center" style="width: 8%">Required</th>
            <th class="text-center" style="width: 7%">Default</th>
            <th class="text-center" style="width: 10%">Type</th>
            <th class="text-center" style="width: 50%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <h5>timeout</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">30000</td>
            <td>Long</td>
            <td>Request timeout in milliseconds.</td>
        </tr>
        <tr>
            <td>
                <h5>max-retries</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">3</td>
            <td>Integer</td>
            <td>Maximum number of retries for failed requests.</td>
        </tr>
        <tr>
            <td>
                <h5>batch-size</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">1</td>
            <td>Integer</td>
            <td>Batch size for inference requests.</td>
        </tr>
        <tr>
            <td>
                <h5>flatten-batch-dim</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">false</td>
            <td>Boolean</td>
            <td>Whether to flatten the batch dimension for array inputs. When true, shape [1,N] becomes [N]. Defaults to false. Useful for Triton models that do not expect a batch dimension.</td>
        </tr>
        <tr>
            <td>
                <h5>priority</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Integer</td>
            <td>Request priority level (0-255). Higher values indicate higher priority.</td>
        </tr>
        <tr>
            <td>
                <h5>sequence-id</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Sequence ID for stateful models.</td>
        </tr>
        <tr>
            <td>
                <h5>sequence-start</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">false</td>
            <td>Boolean</td>
            <td>Whether this is the start of a sequence for stateful models.</td>
        </tr>
        <tr>
            <td>
                <h5>sequence-end</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">false</td>
            <td>Boolean</td>
            <td>Whether this is the end of a sequence for stateful models.</td>
        </tr>
        <tr>
            <td>
                <h5>binary-data</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">false</td>
            <td>Boolean</td>
            <td>Whether to use binary data transfer. Defaults to false (JSON).</td>
        </tr>
        <tr>
            <td>
                <h5>compression</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Compression algorithm to use (e.g., 'gzip').</td>
        </tr>
        <tr>
            <td>
                <h5>auth-token</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Authentication token for secured Triton servers.</td>
        </tr>
        <tr>
            <td>
                <h5>custom-headers</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Custom HTTP headers in JSON format, e.g., <code>{"X-Custom-Header":"value"}</code>.</td>
        </tr>
    </tbody>
</table>

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
4. **Resource Management**: Automatic cleanup of HTTP resources
5. **Timeout Configuration**: Set appropriate timeout values based on model complexity
6. **Retry Strategy**: Configure retry attempts for handling transient failures

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

## Dependencies

To use the Triton model function, you need to include the following dependency in your Flink application:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-model-triton</artifactId>
    <version>${flink.version}</version>
</dependency>
```

{{< top >}}

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

## 概述

Triton 模型函数允许 Flink SQL 调用 [NVIDIA Triton 推理服务器](https://github.com/triton-inference-server/server)进行实时模型推理任务。Triton 推理服务器是一个高性能推理服务解决方案，支持多种机器学习框架，包括 TensorFlow、PyTorch、ONNX、TensorRT 等。

主要特性：
* **高性能**：针对低延迟和高吞吐量推理进行了优化
* **多框架支持**：支持来自各种 ML 框架的模型
* **异步处理**：非阻塞推理请求，实现更好的资源利用
* **灵活配置**：针对不同用例的全面配置选项
* **资源管理**：高效的 HTTP 客户端池和自动资源清理
* **容错能力**：内置重试机制，可配置重试次数

{{< hint info >}}
`flink-model-triton` 模块自 Flink 2.0 起可用。请确保您可以访问正在运行的 Triton 推理服务器实例。
{{< /hint >}}

## 使用示例

### 示例 1：文本分类（基础）

此示例演示对电影评论进行情感分析：

{{< tabs "text-classification" >}}
{{< tab "SQL" >}}
```sql
-- 创建 Triton 模型
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

-- 准备源数据
CREATE TEMPORARY VIEW movie_reviews(id, movie_name, user_review, actual_sentiment)
AS VALUES
  (1, 'Great Movie', 'This movie was absolutely fantastic! Great acting and storyline.', 'positive'),
  (2, 'Boring Film', 'I fell asleep halfway through. Very disappointing.', 'negative'),
  (3, 'Average Show', 'It was okay, nothing special but not terrible either.', 'neutral');

-- 创建输出表
CREATE TEMPORARY TABLE classified_reviews(
  id BIGINT,
  movie_name VARCHAR,
  predicted_sentiment VARCHAR,
  actual_sentiment VARCHAR
) WITH (
  'connector' = 'print'
);

-- 分类情感
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

// 注册模型
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

// 注册源表
tEnv.executeSql(
    "CREATE TEMPORARY VIEW movie_reviews(id, movie_name, user_review, actual_sentiment) " +
    "AS VALUES " +
    "  (1, 'Great Movie', 'This movie was absolutely fantastic!', 'positive'), " +
    "  (2, 'Boring Film', 'I fell asleep halfway through.', 'negative')"
);

// 执行分类
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

### 示例 2：流式图像分类

使用 ResNet 模型对来自 Kafka 流的图像进行分类：

```sql
-- 注册图像分类模型
CREATE MODEL image_classifier
INPUT (image_pixels ARRAY<FLOAT>)
OUTPUT (predicted_class STRING, confidence FLOAT)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'resnet50',
  'model-version' = '1',
  'timeout' = '10000',
  'compression' = 'gzip'  -- 为大型图像数据启用压缩
);

-- 来自 Kafka 的源表
CREATE TEMPORARY TABLE image_stream (
  image_id STRING,
  image_pixels ARRAY<FLOAT>,  -- 预处理后的图像作为浮点数组
  upload_time TIMESTAMP(3),
  WATERMARK FOR upload_time AS upload_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'images',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

-- 分类图像
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

### 示例 3：实时欺诈检测

使用高优先级推理进行欺诈检测：

```sql
-- 创建高优先级的欺诈检测模型
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
  'priority' = '200',  -- 关键交易的高优先级
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

-- 标记可疑交易
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
WHERE fraud_score > 0.5;  -- 对可疑交易发出警报
```

### 示例 4：推荐系统

基于用户行为的产品推荐：

```sql
-- 注册推荐模型
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
  'batch-size' = '4'  -- 批处理多个请求
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

-- 生成个性化推荐
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

### 示例 5：命名实体识别（NER）

从文本文档中提取实体：

```sql
-- 注册 NER 模型，为大型文档启用压缩
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

-- 提取命名实体
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

### 示例 6：有状态序列模型

使用带序列跟踪的有状态模型（RNN/LSTM）：

```sql
-- 注册有状态对话模型
CREATE MODEL conversation_model
INPUT (message_text STRING)
OUTPUT (bot_response STRING)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'chatbot_lstm',
  'sequence-id' = 'conv-001',  -- 唯一序列 ID
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

-- 处理带上下文的对话
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

### 示例 7：批量推理

对历史数据执行批量推理：

```sql
-- 注册批处理模型
CREATE MODEL batch_classifier
INPUT (features ARRAY<DOUBLE>)
OUTPUT (prediction STRING, confidence DOUBLE)
WITH (
  'provider' = 'triton',
  'endpoint' = 'http://triton-server:8000/v2/models',
  'model-name' = 'classifier',
  'timeout' = '60000',
  'batch-size' = '32'  -- 大批量以提高效率
);

-- 批量源表
CREATE TEMPORARY TABLE historical_data (
  id BIGINT,
  features ARRAY<DOUBLE>
) WITH (
  'connector' = 'filesystem',
  'path' = 'hdfs:///data/historical',
  'format' = 'parquet'
);

-- 批量推理，结果写入接收器
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

### 示例 8：安全的 Triton 服务器

访问带身份验证的安全 Triton 服务器：

```sql
-- 注册带身份验证的模型
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
切勿在 SQL 中硬编码敏感令牌。使用 Flink 的密钥管理或环境变量。
{{< /hint >}}

### 示例 9：数组类型与展平批次维度

对于接受数组输入但不需要批次维度的模型：

```sql
-- 创建带数组输入和展平批次维度的模型
CREATE MODEL triton_vector_model
INPUT (input_vector ARRAY<FLOAT>)
OUTPUT (output_vector ARRAY<FLOAT>)
WITH (
    'provider' = 'triton',
    'endpoint' = 'http://localhost:8000/v2/models',
    'model-name' = 'vector-transform',
    'model-version' = '1',
    'flatten-batch-dim' = 'true'  -- 将 [1,N] 展平为 [N]
);

-- 使用模型进行推理
CREATE TEMPORARY TABLE vector_input (
    id BIGINT,
    features ARRAY<FLOAT>
) WITH (
    'connector' = 'datagen',
    'fields.features.length' = '128'  -- 128 维向量
);

SELECT id, output_vector
FROM ML_PREDICT(
  TABLE vector_input,
  MODEL triton_vector_model,
  DESCRIPTOR(features)
);
```

### 示例 10：高级配置

用于生产环境的综合设置：

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

## 模型选项

### 必需选项

{{< generated/triton_common_section >}}

### 可选选项

{{< generated/triton_advanced_section >}}

## Schema 要求

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-center">输入类型</th>
            <th class="text-center">输出类型</th>
            <th class="text-left">说明</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>BOOLEAN, TINYINT, SMALLINT, INT, BIGINT</td>
            <td>BOOLEAN, TINYINT, SMALLINT, INT, BIGINT</td>
            <td>整数类型推理</td>
        </tr>
        <tr>
            <td>FLOAT, DOUBLE</td>
            <td>FLOAT, DOUBLE</td>
            <td>浮点类型推理</td>
        </tr>
        <tr>
            <td>STRING</td>
            <td>STRING</td>
            <td>文本到文本推理（分类、生成等）</td>
        </tr>
        <tr>
            <td>ARRAY&lt;numeric types&gt;</td>
            <td>ARRAY&lt;numeric types&gt;</td>
            <td>数组推理（向量、张量等）。支持数值类型的数组。</td>
        </tr>
    </tbody>
</table>

**注意**：输入和输出类型必须与 Triton 模型配置中定义的类型匹配。

要验证模型的预期输入/输出类型，请查询 Triton 服务器：
```bash
curl http://triton-server:8000/v2/models/{model_name}/config
```

## Triton 服务器设置

要使用此集成，您需要一个正在运行的 Triton 推理服务器。以下是基本设置指南：

### 使用 Docker

```bash
# 拉取 Triton 服务器镜像
docker pull nvcr.io/nvidia/tritonserver:23.10-py3

# 使用您的模型存储库运行 Triton 服务器
docker run --rm -p 8000:8000 -p 8001:8001 -p 8002:8002 \
  -v /path/to/your/model/repository:/models \
  nvcr.io/nvidia/tritonserver:23.10-py3 \
  tritonserver --model-repository=/models
```

### 模型存储库结构

您的模型存储库应遵循以下结构：

```
model_repository/
├── text-classification/
│   ├── config.pbtxt
│   └── 1/
│       └── model.py  # 或 model.onnx、model.plan 等
└── other-model/
    ├── config.pbtxt
    └── 1/
        └── model.savedmodel/
```

### 示例模型配置

以下是文本分类模型的 `config.pbtxt` 示例：

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

## 性能注意事项

1. **连接池**：HTTP 客户端被池化和重用以提高效率
2. **异步处理**：非阻塞请求防止线程饥饿
3. **批处理**：配置批大小以实现最佳吞吐量
   - 简单模型：batch-size 1-4
   - 中等模型：batch-size 4-16
   - 复杂模型：batch-size 16-32
4. **资源管理**：HTTP 资源的自动清理
5. **超时配置**：根据模型复杂度设置适当的超时值
   - 简单模型：1-5 秒
   - 中等模型（如 BERT）：5-30 秒
   - 复杂模型（如 GPT）：30-120 秒
6. **重试策略**：配置重试次数以处理瞬态故障
7. **压缩**：对于 > 1KB 的负载启用 gzip 压缩
8. **并行度**：将 Flink 并行度与 Triton 服务器容量匹配

## 最佳实践

### 模型版本管理

在生产环境中固定模型版本以确保一致性：
```sql
'model-version' = '3'  -- 固定到版本 3 而不是 'latest'
```

### 错误处理

失败时使用默认值：
```sql
SELECT COALESCE(output, 'UNKNOWN') AS prediction
FROM ML_PREDICT(...)
```

### 资源配置

为高吞吐量场景配置足够的内存和网络缓冲区：
```yaml
taskmanager.memory.managed.size: 2gb
taskmanager.network.memory.fraction: 0.2
```

## 错误处理

该集成包括全面的错误处理：

- **连接错误**：使用指数退避自动重试
- **超时处理**：可配置的请求超时
- **HTTP 错误**：来自 Triton 服务器的详细错误消息
- **序列化错误**：JSON 解析和验证错误

## 监控和调试

启用调试日志以监控集成：

```properties
# 在 log4j2.properties 中
logger.triton.name = org.apache.flink.model.triton
logger.triton.level = DEBUG
```

这将提供有关以下内容的详细日志：
- HTTP 请求/响应详细信息
- 客户端连接管理
- 错误条件和重试
- 性能指标

## 故障排查

### 连接问题
- 验证 Triton 服务器正在运行：`curl http://triton-server:8000/v2/health/ready`
- 检查网络连接和防火墙规则
- 确保端点 URL 包含正确的协议（http/https）

### 超时错误
- 增加超时值：`'timeout' = '60000'`
- 检查 Triton 服务器资源使用情况（CPU/GPU）
- 监控 Triton 服务器日志以查找慢速模型执行

### 类型不匹配
- 验证模型 schema：`curl http://triton-server:8000/v2/models/{model}/config`
- 显式转换 Flink 类型：`CAST(value AS FLOAT)`
- 确保数组维度与模型期望匹配

### 高延迟
- 启用请求压缩：`'compression' = 'gzip'`
- 增加 Triton 服务器实例
- 在 Triton 服务器配置中使用动态批处理
- 检查 Flink 和 Triton 之间的网络延迟

## 依赖项

要使用 Triton 模型函数，您需要在 Flink 应用程序中包含以下依赖项：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-model-triton</artifactId>
    <version>${flink.version}</version>
</dependency>
```

## 更多信息

- [Triton 推理服务器文档](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/)
- [Triton 模型配置](https://github.com/triton-inference-server/server/blob/main/docs/user_guide/model_configuration.md)
- [Flink 异步 I/O]({{< ref "docs/dev/datastream/operators/asyncio" >}})
- [Flink 指标]({{< ref "docs/ops/metrics" >}})

{{< top >}}
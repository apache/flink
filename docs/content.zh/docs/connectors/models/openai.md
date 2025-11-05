---
title: "OpenAI"
weight: 1
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

# OpenAI

OpenAI模型函数允许Flink SQL调用[OpenAI API](https://platform.openai.com/docs/overview)执行推理任务。

## 概述

该函数支持通过Flink SQL调用远程的OpenAI模型服务进行预测/推理任务。目前支持以下任务类型：

* [Chat Completions](https://platform.openai.com/docs/api-reference/chat)：根据包含对话消息列表生成模型响应。
* [Embeddings](https://platform.openai.com/docs/api-reference/embeddings)：获取给定输入的向量表示，方便在后续流程中由机器学习模型和算法消费。

## 使用示例

以下示例创建了一个聊天补全模型，并使用它对电影评论进行情感标签预测。

首先，使用如下SQL语句创建聊天补全模型：

```sql
CREATE MODEL ai_analyze_sentiment
INPUT (`input` STRING)
OUTPUT (`content` STRING)
WITH (
    'provider'='openai',
    'endpoint'='https://api.openai.com/v1/chat/completions',
    'api-key' = '<YOUR KEY>',
    'model'='gpt-3.5-turbo',
    'system-prompt' = 'Classify the text below into one of the following labels: [positive, negative, neutral, mixed]. Output only the label.'
);
```

假设如下数据存储在名为 `movie_comment` 的表中，预测结果需要存储到名为 `print_sink` 的表中：

```sql
CREATE TEMPORARY VIEW movie_comment(id, movie_name,  user_comment, actual_label)
AS VALUES
  (1, '好东西', '最爱小孩子猜声音那段，算得上看过的电影里相当浪漫的叙事了。很温和也很有爱。', 'positive');

CREATE TEMPORARY TABLE print_sink(
  id BIGINT,
  movie_name VARCHAR,
  predicit_label VARCHAR,
  actual_label VARCHAR
) WITH (
  'connector' = 'print'
);
```

然后就可以使用如下SQL语句对电影评论进行情感标签预测。

```sql
INSERT INTO print_sink
SELECT id, movie_name, content as predicit_label, actual_label
FROM ML_PREDICT(
  TABLE movie_comment,
  MODEL ai_analyze_sentiment,
  DESCRIPTOR(user_comment));
```

## 模型选项

### 公共选项

{{< generated/model_openai_common_section >}}

### Chat Completions

{{< generated/model_openai_chat_section >}}

### Embeddings

{{< generated/model_openai_embedding_section >}}

## Schema要求

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-center">任务类型</th>
            <th class="text-left">输入类型</th>
            <th class="text-center">输出类型</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Chat Completions</td>
            <td>STRING</td>
            <td>STRING</td>
        </tr>
        <tr>
            <td>Embeddings</td>
            <td>STRING</td>
            <td>ARRAY&lt;FLOAT&gt;</td>
        </tr>
    </tbody>
</table>

### 可用元数据

当配置 `error-handling-strategy` 为 `ignore` 时，您可以选择额外指定以下元数据列，将故障信息展示到您的输出流中。

* error-string(STRING)：与错误相关的消息
* http-status-code(INT)：HTTP状态码
* http-headers-map(MAP<STRING, ARRAY<STRING>>)：响应返回的头部信息

如果您在Output Schema中定义了这些元数据列，但调用未失败，则这些列将填充为null值。

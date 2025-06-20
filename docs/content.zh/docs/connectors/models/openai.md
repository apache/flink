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

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">参数</th>
            <th class="text-center" style="width: 10%">是否必选</th>
            <th class="text-center" style="width: 10%">默认值</th>
            <th class="text-center" style="width: 10%">数据类型</th>
            <th class="text-center" style="width: 45%">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <h5>provider</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>指定使用的模型提供方，必须为 'openai'。</td>
        </tr>
        <tr>
            <td>
                <h5>endpoint</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>OpenAI API端点的完整URL，例如：<code>https://api.openai.com/v1/chat/completions</code> 或
                <code>https://api.openai.com/v1/embeddings</code>。</td>
        </tr>
        <tr>
            <td>
                <h5>api-key</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>用于认证的OpenAI API密钥。</td>
        </tr>
        <tr>
            <td>
                <h5>model</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>模型名称，例如：<code>gpt-3.5-turbo</code>, <code>text-embedding-ada-002</code>。</td>
        </tr>
    </tbody>
</table>

### Chat Completions

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">参数</th>
            <th class="text-center" style="width: 10%">是否必选</th>
            <th class="text-center" style="width: 10%">默认值</th>
            <th class="text-center" style="width: 10%">数据类型</th>
            <th class="text-center" style="width: 45%">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <h5>system-prompt</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">"You are a helpful assistant."</td>
            <td>String</td>
            <td>用于聊天任务的系统提示信息。</td>
        </tr>
        <tr>
            <td>
                <h5>temperature</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Double</td>
            <td>控制输出的随机性，取值范围<code>[0.0, 1.0]</code>。参考<a href="https://platform.openai.com/docs/api-reference/chat/create#chat-create-temperature">temperature</a></td>
        </tr>
        <tr>
            <td>
                <h5>top-p</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Double</td>
            <td>用于替代temperature的概率阈值。参考<a href="https://platform.openai.com/docs/api-reference/chat/create#chat-create-top_p">top_p</a></td>
        </tr>
        <tr>
            <td>
                <h5>stop</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>String</td>
            <td>停止序列，逗号分隔的列表。参考<a href="https://platform.openai.com/docs/api-reference/chat/create#chat-create-stop">stop</a></td>
        </tr>
        <tr>
            <td>
                <h5>max-tokens</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Long</td>
            <td>生成的最大token数。参考<a href="https://platform.openai.com/docs/api-reference/chat/create#chat-create-max_tokens">max tokens</a></td>
        </tr>
    </tbody>
</table>

### Embeddings

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">参数</th>
            <th class="text-center" style="width: 10%">是否必选</th>
            <th class="text-center" style="width: 10%">默认值</th>
            <th class="text-center" style="width: 10%">数据类型</th>
            <th class="text-center" style="width: 45%">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <h5>dimension</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Long</td>
            <td>embedding向量的维度。参考<a href="https://platform.openai.com/docs/api-reference/embeddings/create#embeddings-create-dimensions">dimensions</a></td>
        </tr>
    </tbody>
</table>

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

---
title: "Deepseek"
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

# DeepSeek

DeepSeek模型函数允许Flink SQL调用[DeepSeek API](https://api-docs.deepseek.com/zh-cn/)执行推理任务。

## 概述

该函数支持通过Flink SQL调用远程的DeepSeek模型服务进行预测/推理任务。目前支持以下任务类型：

* [deepseek-chat](https://platform.DeepSeek.com/docs/api-reference/chat)：根据包含对话消息列表生成模型响应。
* [deepseek-reasoner](https://api-docs.deepseek.com/zh-cn/guides/reasoning_model)：获取给定输入的向量表示，方便在后续流程中由机器学习模型和算法消费。

## 使用示例

以下示例创建了一个对话模型，并使用它来降低敏感信息的敏感度。
首先，使用以下SQL语句创建聊天完成模型：

```sql
CREATE MODEL ai_analyze_sentiment
INPUT (`input` STRING)
OUTPUT (`content` STRING)
WITH (
    'provider'='DeepSeek',
    'endpoint'='https://api.DeepSeek.com/v1/chat/completions',
    'api-key' = '<YOUR KEY>',
    'model'='gpt-3.5-turbo',
    'system-prompt' = '将内容中的身份证号 后四位用*号代替.'
);
```

假设以下数据存储在名为`user_info`的表中，则预测结果将存储在名为`print_sink` 的表中：

```sql
CREATE TEMPORARY VIEW user_info(id, user_name,  info, actual_label)
AS VALUES
  (1, 'aaa', '我的名字叫 码界探索: 412721199803134419.', 'positive');

CREATE TEMPORARY TABLE print_sink(
  id BIGINT,
  user_name VARCHAR,
  desensitized_data VARCHAR,
  actual_label VARCHAR
) WITH (
  'connector' = 'print'
);
```

然后就可以使用如下SQL语句对电影评论进行情感标签预测。

```sql
INSERT INTO print_sink
SELECT id, user_name, content as desensitized_data, actual_label
FROM ML_PREDICT(
    TABLE user_info,
        MODEL ai_data_desensitization,
    DESCRIPTOR(info));
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
            <td>指定使用的模型提供方，必须为 'DeepSeek'。</td>
        </tr>
        <tr>
            <td>
                <h5>endpoint</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>DeepSeek API端点的完整URL，例如：<code>https://api.deepSeek.com/v1</code> 。</td>
        </tr>
        <tr>
            <td>
                <h5>api-key</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>用于认证的DeepSeek API密钥。</td>
        </tr>
        <tr>
            <td>
                <h5>model</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>模型名称，例如：<code>deepseek-chat </code>, <code>deepseek-reasoner</code>。</td>
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
            <td>控制输出的随机性，取值范围<code>[0.0, 2.0]</code>。参考<a href="https://api-docs.deepseek.com/zh-cn/">temperature</a></td>
        </tr>
        <tr>
            <td>
                <h5>top-p</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Double</td>
            <td>用于替代temperature的概率阈值。参考<a href="https://api-docs.deepseek.com/zh-cn/">top_p</a></td>
        </tr>
        <tr>
            <td>
                <h5>stop</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>String</td>
            <td>停止序列，逗号分隔的列表。参考<a href="https://api-docs.deepseek.com/zh-cn/">stop</a></td>
        </tr>
        <tr>
            <td>
                <h5>max-tokens</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Long</td>
            <td>介于 1 到 8192 间的整数，限制一次请求中模型生成 completion 的最大 token 数。输入 token 和输出 token 的总长度受模型的上下文长度的限制。

如未指定 max_tokens参数，默认使用 4096。参考<a href="https://api-docs.deepseek.com/zh-cn/">max tokens</a></td>
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
                    <h5>max-tokens</h5>
                </td>
                <td>optional</td>
                <td style="word-wrap: break-word;">null</td>
                <td>Long</td>
                <td>模型单次回答的最大长度（含思维链输出），默认为 32K，最大为 64K。 参考 <a href="https://api-docs.deepseek.com/guides/reasoning_model">max tokens</a></td>
    </tr>
    </tbody>
</table>

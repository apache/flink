---
title: "Qwen"
weight: 1
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  参考 the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  参考 the License for the
specific language governing permissions and limitations
under the License.
-->

# Qwen

Qwen模型函数允许Flink SQL调用Qwen API(https://www.alibabacloud.com/help/zh/model-studio/toolkits-and-frameworks/)执行推理任务。
内部是用的是OpenAI SDK对其进行支持.

## 概述

该函数支持通过Flink SQL调用远程的Qwen模型服务进行预测/推理任务。目前支持以下任务类型：


* [qwen-plus](https://platform.openai.com/docs/api-reference/chat)：根据包含对话消息列表生成模型响应。
* [text-embedding-v4](https://www.alibabacloud.com/help/zh/model-studio/text-embedding-synchronous-api)：获取给定输入的向量表示，方便在后续流程中由机器学习模型和算法消费。


## 使用示例

以下示例创建了一个对话模型，并使用它来降低敏感信息的敏感度。
首先，使用以下SQL语句创建聊天完成模型：

```sql
-- Create chat model
CREATE MODEL ai_data_desensitization
INPUT (input STRING)
OUTPUT (content STRING)
WITH (
  'provider' = 'qwen',
  'endpoint' = 'https://dashscope.aliyuncs.com/compatible-mode/v1',
  'api-key' = 'your-api-key',
  'model' = 'qwen-plus',
  'system-prompt' = '将内容中的身份证号 后四位用*号代替。'

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

然后，您可以使用以下SQL语句来脱敏身份证号码：

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
            <td>指定使用的模型提供方，必须为  'qwen'。</td>
        </tr>
        <tr>
            <td>
                <h5>endpoint</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Qwen API端点的完整URL，例如： <code>https://dashscope.aliyuncs.com/compatible-mode/v1</code>
               </td>
        </tr>
        <tr>
            <td>
                <h5>api-key</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>用于认证的Qwen API密钥。</td>
        </tr>
        <tr>
            <td>
                <h5>model</h5>
            </td>
            <td>必填</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>模型名称，例如：<code>qwen-plus</code>, <code>text-embedding-v4</code>。</td>
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
            <td style="word-wrap: break-word;">1</td>
            <td>Double</td>
            <td>控制输出的随机性，取值范围<code>[0, 2)</code>  参考 <a href="https://www.alibabacloud.com/help/zh/model-studio/use-qwen-by-calling-api">temperature</a></td>
        </tr>
        <tr>
            <td>
                <h5>top_p</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">1</td>
            <td>Double</td>
            <td>核采样的概率阈值，控制模型生成文本的多样性。取值范围：（0,1.0]。 参考 <a href="https://www.alibabacloud.com/help/zh/model-studio/use-qwen-by-calling-api">top_p</a></td>
        </tr>
        <tr>
            <td>
                <h5>stop</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>String</td>
            <td>使用stop参数后，当模型生成的文本即将包含指定的字符串或token_id时，将自动停止生成。参考 <a href="https://www.alibabacloud.com/help/zh/model-studio/use-qwen-by-calling-api">stop</a></td>
        </tr>
        <tr>
            <td>
                <h5>max_tokens</h5>
            </td>
            <td>可选</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Long</td>
            <td>Integer 本次请求返回的最大 Token 数。 参考 <a href="https://www.alibabacloud.com/help/zh/model-studio/models#9f8890ce29g5u">max tokens</a></td>
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
                <td>单行最大处理Token数。参考 <a href="https://www.alibabacloud.com/help/zh/model-studio/models#9f8890ce29g5u">max tokens</a></td>
    </tr>
    </tbody>
</table>

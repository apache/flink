---
title: "Qwen"
weight: 1
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.   See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.   See the License for the
specific language governing permissions and limitations
under the License.
-->

# Qwen

The Qwen Model Function allows Flink SQL to call (https://www.alibabacloud.com/help/en/model-studio/toolkits-and-frameworks/) for inference tasks.

## Overview

The function supports calling remote Qwen model services via Flink SQL for prediction/inference tasks. Currently, the following tasks are supported:


* [qwen-plus](https://www.alibabacloud.com/help/en/model-studio/use-qwen-by-calling-api)：generate a model response from a list of messages comprising a conversation.
* [text-embedding-v4](https://www.alibabacloud.com/help/en/model-studio/text-embedding-synchronous-api)：get a vector representation of a given input that can be easily consumed by machine learning models and algorithms.


## Usage Example
The following example creates a dialogue model and uses it to desensitize sensitive information.

First, create the chat completions model with the following SQL statement:

```sql
-- Create chat model
CREATE MODEL ai_data_desensitization
INPUT (input STRING)
OUTPUT (content STRING)
WITH (
  'provider' = 'deepseek',
  'endpoint' = 'https://api.deepseek.com/v1',
  'api-key' = 'your-api-key',
  'model' = 'deepseek-chat',
  'system-prompt' = 'Please desensitize the last four digits of the entered data ID number into*.'

);
```

Assuming the following data is stored in a table named `user_info`, the prediction results will be stored in a table named `print_sink`:

```sql
CREATE TEMPORARY VIEW user_info(id, user_name,  info, actual_label)
AS VALUES
  (1, 'mj', 'My name is mj, ID number number: 412721199803135419.', 'positive');

CREATE TEMPORARY TABLE print_sink(
  id BIGINT,
  user_name VARCHAR,
  predicit_label VARCHAR,
  actual_label VARCHAR
) WITH (
  'connector' = 'print'
);
```

Then, you can use the following SQL statements to desensitize the ID card number:

```sql
INSERT INTO print_sink
SELECT id, user_name, content as predicit_label, actual_label
FROM ML_PREDICT(
    TABLE user_info,
        MODEL ai_data_desensitization,
    DESCRIPTOR(info));
```
## Model Options

### Common

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
            <td>Specifies the model function provider to use, must be 'qwen'.</td>
        </tr>
        <tr>
            <td>
                <h5>endpoint</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Full URL of the Qwen API endpoint, e.g.  <code>https://dashscope.aliyuncs.com/compatible-mode/v1</code>
               </td>
        </tr>
        <tr>
            <td>
                <h5>api-key</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Qwen API key for authentication.</td>
        </tr>
        <tr>
            <td>
                <h5>model</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Model name, e.g.  <code>qwen-plus</code>, <code>text-embedding-v4</code>.</td>
        </tr>
    </tbody>
</table>

### Chat Completions

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
                <h5>system-prompt</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">"You are a helpful assistant."</td>
            <td>String</td>
            <td>The input message for the system role.</td>
        </tr>
        <tr>
            <td>
                <h5>temperature</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">1</td>
            <td>Double</td>
            <td>Controls randomness of output, range <code>[0, 2)</code>   See <a href="https://www.alibabacloud.com/help/en/model-studio/use-qwen-by-calling-api">temperature</a></td>
        </tr>
        <tr>
            <td>
                <h5>top_p</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">1</td>
            <td>Double</td>
            <td>Probability cutoff for token selection (used instead of temperature) eg （0,1.0]. See.  See <a href="https://www.alibabacloud.com/help/en/model-studio/use-qwen-by-calling-api">top_p</a></td>
        </tr>
        <tr>
            <td>
                <h5>stop</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">null</td>
            <td>String</td>
            <td>If you use the `stop` parameter, the model stops generating text when it encounters the specified string or `token_id`.  See <a href="https://www.alibabacloud.com/help/en/model-studio/use-qwen-by-calling-api">stop</a></td>
        </tr>
        <tr>
            <td>
                <h5>max_tokens</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Long</td>
            <td>The maximum number of tokens to generate for the current request. See <a href="https://www.alibabacloud.com/help/en/model-studio/use-qwen-by-calling-api">max tokens</a></td>
        </tr>
    </tbody>
</table>

### Embeddings

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
                    <h5>max-tokens</h5>
                </td>
                <td>optional</td>
                <td style="word-wrap: break-word;">null</td>
                <td>Long</td>
                <td>Maximum number of tokens processed per line 8,192.  See <a href="https://www.alibabacloud.com/help/en/model-studio/models#9f8890ce29g5u">max tokens</a></td>
    </tr>
    </tbody>
</table>

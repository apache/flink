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

# DeepSeek

The DeepSeek Model Function allows Flink SQL to call [DeepSeek API](https://www.deepseek.com/) for inference tasks.

## Configuration Options

| Option         | Required | Default  | Description                                                         |
|----------------|----------|----------|---------------------------------------------------------------------|
| provider       | Yes      | -        | Must be `deepseek`                                                  |
| endpoint       | Yes      | -        | API endpoint (e.g., `https://api.deepseek.com/v1/chat/completions`) |
| api-key        | Yes      | -        | DeepSeek API key                                                    |
| model          | Yes      | -        | Model name (e.g., `deepseek-chat`、`deepseek-reasoner`)                                 |
| temperature    | No       | 0.7      | Sampling temperature                                                |
| max-tokens     | No       | -        | Maximum output tokens                                               |
| dimension      | No       | -        | Embedding vector dimension                                          |

## Usage Example

```sql
-- Create chat model
CREATE MODEL deepseek_chat
INPUT (input STRING)
OUTPUT (content STRING)
WITH (
  'provider' = 'deepseek',
  'endpoint' = 'https://api.deepseek.com/v1/chat/completions',
  'api-key' = 'your-api-key',
  'model' = 'deepseek-chat',
  'temperature' = '0.5'
);

-- Use model for prediction
SELECT input, content
FROM ML_PREDICT(TABLE source_table, MODEL deepseek_chat, DESCRIPTOR(`input`));

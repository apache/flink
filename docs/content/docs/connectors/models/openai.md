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

The OpenAI Model Function allows Flink SQL to call [OpenAI API](https://platform.openai.com/docs/overview) for inference tasks.

## Overview

The function supports calling remote OpenAI model services via Flink SQL for prediction/inference tasks. Currently, the following tasks are supported:

* [Chat Completions](https://platform.openai.com/docs/api-reference/chat): generate a model response from a list of messages comprising a conversation.
* [Embeddings](https://platform.openai.com/docs/api-reference/embeddings): get a vector representation of a given input that can be easily consumed by machine learning models and algorithms.

## Usage examples

The following example creates a chat completions model and uses it to predict sentiment labels for movie reviews.

First, create the chat completions model with the following SQL statement:

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

Suppose the following data is stored in a table named `movie_comment`, and the prediction result is to be stored in a table named `print_sink`:

```sql
CREATE TEMPORARY VIEW movie_comment(id, movie_name,  user_comment, actual_label)
AS VALUES
  (1, 'Good Stuff', 'The part where children guess the sounds is my favorite. It's a very romantic narrative compared to other movies I've seen. Very gentle and full of love.', 'positive');

CREATE TEMPORARY TABLE print_sink(
  id BIGINT,
  movie_name VARCHAR,
  predicit_label VARCHAR,
  actual_label VARCHAR
) WITH (
  'connector' = 'print'
);
```

Then the following SQL statement can be used to predict sentiment labels for movie reviews:

```sql
INSERT INTO print_sink
SELECT id, movie_name, content as predicit_label, actual_label
FROM ML_PREDICT(
  TABLE movie_comment,
  MODEL ai_analyze_sentiment,
  DESCRIPTOR(user_comment));
```

## Model Options

### Common

{{< generated/model_openai_common_section >}}

### Chat Completions

{{< generated/model_openai_chat_section >}}

### Embeddings

{{< generated/model_openai_embedding_section >}}

## Schema Requirement

The following table lists the schema requirement for each task.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-center">Task</th>
            <th class="text-left">Input Type</th>
            <th class="text-center">Output Type</th>
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

### Available Metadata

When configuring `error-handling-strategy` as `ignore`, you can choose to additionally specify the
following metadata columns to surface information about failures into your stream.

* error-string(STRING): A message associated with the error
* http-status-code(INT): The HTTP status code
* http-headers-map(MAP<STRING, ARRAY<STRING>>): The headers returned with the response

If you defined these metadata columns in the output schema but the call did not fail, the columns 
will be filled with null values.

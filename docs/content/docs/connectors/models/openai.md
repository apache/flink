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
            <td>Specifies the model function provider to use, must be 'openai'.</td>
        </tr>
        <tr>
            <td>
                <h5>endpoint</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Full URL of the OpenAI API endpoint, e.g. <code>https://api.openai.com/v1/chat/completions</code> or
                <code>https://api.openai.com/v1/embeddings</code>.</td>
        </tr>
        <tr>
            <td>
                <h5>api-key</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>OpenAI API key for authentication.</td>
        </tr>
        <tr>
            <td>
                <h5>model</h5>
            </td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Model name, e.g. <code>gpt-3.5-turbo</code>, <code>text-embedding-ada-002</code>.</td>
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
            <td style="word-wrap: break-word;">null</td>
            <td>Double</td>
            <td>Controls randomness of output, range <code>[0.0, 1.0]</code>. See <a href="https://platform.openai.com/docs/api-reference/chat/create#chat-create-temperature">temperature</a></td>
        </tr>
        <tr>
            <td>
                <h5>top-p</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Double</td>
            <td>Probability cutoff for token selection (used instead of temperature). See <a href="https://platform.openai.com/docs/api-reference/chat/create#chat-create-top_p">top_p</a></td>
        </tr>
        <tr>
            <td>
                <h5>stop</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">null</td>
            <td>String</td>
            <td>Stop sequences, comma-separated list. See <a href="https://platform.openai.com/docs/api-reference/chat/create#chat-create-stop">stop</a></td>
        </tr>
        <tr>
            <td>
                <h5>max-tokens</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Long</td>
            <td>Maximum number of tokens to generate. See <a href="https://platform.openai.com/docs/api-reference/chat/create#chat-create-max_tokens">max tokens</a></td>
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
                <h5>dimension</h5>
            </td>
            <td>optional</td>
            <td style="word-wrap: break-word;">null</td>
            <td>Long</td>
            <td>Dimension of the embedding vector. See <a href="https://platform.openai.com/docs/api-reference/embeddings/create#embeddings-create-dimensions">dimensions</a></td>
        </tr>
    </tbody>
</table>

## Schema Requirement

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

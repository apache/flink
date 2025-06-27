---
title: "Model Inference"
weight: 7
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

# Model Inference

{{< label Streaming >}}

Flink SQL provides the `ML_PREDICT` table-valued function (TVF) to perform model inference in SQL queries. This function allows you to apply machine learning models to your data streams directly in SQL.
See [Model Creation]({{< ref "docs/dev/table/sql/create#create-model" >}}) about how to create a model.

## ML_PREDICT Function

The `ML_PREDICT` function takes a table input, applies a model to it, and returns a new table with the model's predictions. The function offers support for synchronous/asynchronous inference modes when the underlying model permits both.

### Syntax

```sql
SELECT * FROM
ML_PREDICT(
  TABLE input_table,
  MODEL model_name,
  DESCRIPTOR(feature_columns),
  [CONFIG => MAP['key', 'value']]
)
```

### Parameters

- `input_table`: The input table containing the data to be processed
- `model_name`: The name of the model to use for inference
- `feature_columns`: A descriptor specifying which columns from the input table should be used as features for the model
- `config`: (Optional) A map of configuration options for the model inference

### Configuration Options

The following configuration options can be specified in the config map:

{{< generated/ml_predict_runtime_config_configuration >}}

### Example

```sql
-- Basic usage
SELECT * FROM ML_PREDICT(
  TABLE input_table,
  MODEL my_model,
  DESCRIPTOR(feature1, feature2)
);

-- With configuration options
SELECT * FROM ML_PREDICT(
  TABLE input_table,
  MODEL my_model,
  DESCRIPTOR(feature1, feature2),
  MAP['async', 'true', 'timeout', '100s']
);

-- Using named parameters
SELECT * FROM ML_PREDICT(
  INPUT => TABLE input_table,
  MODEL => MODEL my_model,
  ARGS => DESCRIPTOR(feature1, feature2),
  CONFIG => MAP['async', 'true']
);
```

### Output

The output table contains all columns from the input table plus the model's prediction columns. The prediction columns are added based on the model's output schema.

### Notes

1. The model must be registered in the catalog before it can be used with `ML_PREDICT`.
2. The number of feature columns specified in the descriptor must match the model's input schema.
3. If column names in the output conflict with existing column names in the input table, an index will be added to the output column names to avoid conflicts. For example, if the output column is named `prediction`, it will be renamed to `prediction0` if a column with that name already exists in the input table.
4. For asynchronous inference, the model provider must support the `AsyncPredictRuntimeProvider` interface.
5. `ML_PREDICT` only supports append-only tables. CDC (Change Data Capture) tables are not supported because `ML_PREDICT` results are non-deterministic.

### Model Provider

The `ML_PREDICT` function uses a `ModelProvider` to perform the actual model inference. The provider is looked up based on the provider identifier specified when registering the model. There are two types of model providers:

1. `PredictRuntimeProvider`: For synchronous model inference
   - Implements the `createPredictFunction` method to create a synchronous prediction function
   - Used when `async` is set to `false` in the config

2. `AsyncPredictRuntimeProvider`: For asynchronous model inference
   - Implements the `createAsyncPredictFunction` method to create an asynchronous prediction function
   - Used when `async` is set to `true` in the config
   - Requires additional configuration for timeout and buffer capacity

If `async` is not set in the config, the system will pick either sync or async model provider and prefer async model provider if both exist.

### Error Handling

The function will throw an exception in the following cases:
- The model does not exist in the catalog
- The number of feature columns does not match the model's input schema
- The model parameter is missing
- Too few or too many arguments are provided

### Performance Considerations

1. For high-throughput scenarios, consider using asynchronous inference mode.
2. Configure appropriate timeout and buffer capacity values for asynchronous inference.
3. The function's performance depends on the underlying model provider implementation.

### Related Statements

- [Model Creation]({{< ref "docs/dev/table/sql/create#create-model" >}})
- [Model Alteration]({{< ref "docs/dev/table/sql/alter#alter-model" >}})

{{< top >}} 

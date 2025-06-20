---
title: "模型推理"
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

# 模型推理

{{< label Streaming >}}

Flink SQL 提供了 `ML_PREDICT` 表值函数（TVF）来在 SQL 查询中执行模型推理。该函数允许您直接在 SQL 中对数据流应用机器学习模型。
请参阅[模型创建]({{< ref "docs/dev/table/sql/create#create-model" >}})了解如何创建模型。

## ML_PREDICT 函数

`ML_PREDICT` 函数接收一个表输入，对其应用模型，并返回一个包含模型预测结果的新表。当底层模型允许时，该函数提供同步/异步推理模式支持。

### 语法

```sql
SELECT * FROM
ML_PREDICT(
  TABLE input_table,
  MODEL model_name,
  DESCRIPTOR(feature_columns),
  [CONFIG => MAP['key', 'value']]
)
```

### 参数

- `input_table`：包含待处理数据的输入表
- `model_name`：用于推理的模型名称
- `feature_columns`：指定输入表中哪些列应作为模型特征的描述符
- `config`：（可选）模型推理的配置选项映射

### 配置选项

以下配置选项可以在配置映射中指定：

{{< generated/ml_predict_runtime_config_configuration >}}

### 示例

```sql
-- 基本用法
SELECT * FROM ML_PREDICT(
  TABLE input_table,
  MODEL my_model,
  DESCRIPTOR(feature1, feature2)
);

-- 使用配置选项
SELECT * FROM ML_PREDICT(
  TABLE input_table,
  MODEL my_model,
  DESCRIPTOR(feature1, feature2),
  MAP['async', 'true', 'timeout', '100s']
);

-- 使用命名参数
SELECT * FROM ML_PREDICT(
  INPUT => TABLE input_table,
  MODEL => MODEL my_model,
  ARGS => DESCRIPTOR(feature1, feature2),
  CONFIG => MAP['async', 'true']
);
```

### 输出

输出表包含输入表的所有列以及模型的预测列。预测列根据模型的输出模式添加。

### 注意事项

1. 在使用 `ML_PREDICT` 之前，模型必须在目录中注册。
2. 描述符中指定的特征列数量必须与模型的输入模式匹配。
3. 如果输出中的列名与输入表中的现有列名冲突，将在输出列名中添加索引以避免冲突。例如，如果输出列名为 `prediction`，且输入表中已存在同名列，则输出列将被重命名为 `prediction0`。
4. 对于异步推理，模型提供者必须支持 `AsyncPredictRuntimeProvider` 接口。
5. `ML_PREDICT` 仅支持仅追加表。不支持 CDC（变更数据捕获）表，因为 `ML_PREDICT` 的结果是非确定性的。

### 模型提供者

`ML_PREDICT` 函数使用 `ModelProvider` 执行实际的模型推理。提供者根据注册模型时指定的提供者标识符进行查找。有两种类型的模型提供者：

1. `PredictRuntimeProvider`：用于同步模型推理
   - 实现 `createPredictFunction` 方法以创建同步预测函数
   - 当配置中 `async` 设置为 `false` 时使用

2. `AsyncPredictRuntimeProvider`：用于异步模型推理
   - 实现 `createAsyncPredictFunction` 方法以创建异步预测函数
   - 当配置中 `async` 设置为 `true` 时使用
   - 需要额外的超时和缓冲区容量配置

如果配置中未设置 `async`，系统将选择同步或异步模型提供者，如果两者都存在，则优先使用异步模型提供者。

### 错误处理

在以下情况下，函数将抛出异常：
- 模型中不存在于目录中
- 特征列数量与模型的输入模式不匹配
- 缺少模型参数
- 提供的参数过少或过多

### 性能考虑

1. 对于高吞吐量场景，考虑使用异步推理模式。
2. 为异步推理配置适当的超时和缓冲区容量值。
3. 函数的性能取决于底层模型提供者的实现。

### 相关语句

- [模型创建]({{< ref "docs/dev/table/sql/create#create-model" >}})
- [模型修改]({{< ref "docs/dev/table/sql/alter#alter-model" >}})

{{< top >}}

---
title: "概览"
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

# User-defined Functions

PyFlink Table API empowers users to do data transformations with Python user-defined functions.

Currently, it supports two kinds of Python user-defined functions: the [general Python user-defined
functions]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}) which process data one row at a time and
[vectorized Python user-defined functions]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}})
which process data one batch at a time.

## 打包 UDFs

如果你在非 local 模式下运行 Python UDFs 和 Pandas UDFs，且 Python UDFs 没有定义在含 `main()` 入口的 Python 主文件中，强烈建议你通过 [`python-files`]({{< ref "docs/dev/python/python_config" >}}#python-files) 配置项指定 Python UDF 的定义。
否则，如果你将 Python UDFs 定义在名为 `my_udf.py` 的文件中，你可能会遇到 `ModuleNotFoundError: No module named 'my_udf'` 这样的报错。

## 在 UDF 中载入资源

有时候，我们想在 UDF 中只载入一次资源，然后反复使用该资源进行计算。例如，你想在 UDF 中首先载入一个巨大的深度学习模型，然后使用该模型多次进行预测。

你要做的是重载 `UserDefinedFunction` 类的 `open` 方法。

```python
class Predict(ScalarFunction):
    def open(self, function_context):
        import pickle

        with open("resources.zip/resources/model.pkl", "rb") as f:
            self.model = pickle.load(f)

    def eval(self, x):
        return self.model.predict(x)

predict = udf(Predict(), result_type=DataTypes.DOUBLE(), func_type="pandas")
```

## 访问作业参数

The `open()` method provides a `FunctionContext` that contains information about the context in which
user-defined functions are executed, such as the metric group, the global job parameters, etc.

The following information can be obtained by calling the corresponding methods of `FunctionContext`:

| Method                                   | Description                                                             |
| :--------------------------------------- | :---------------------------------------------------------------------- |
| `get_metric_group()`                       | Metric group for this parallel subtask.                                 |
| `get_job_parameter(name, default_value)`    | Global job parameter value associated with given key.                   |

```python
class HashCode(ScalarFunction):

    def open(self, function_context: FunctionContext):
        # access the global "hashcode_factor" parameter
        # "12" would be the default value if the parameter does not exist
        self.factor = int(function_context.get_job_parameter("hashcode_factor", "12"))

    def eval(self, s: str):
        return hash(s) * self.factor

hash_code = udf(HashCode(), result_type=DataTypes.INT())
TableEnvironment t_env = TableEnvironment.create(...)
t_env.get_config().set('pipeline.global-job-parameters', 'hashcode_factor:31')
t_env.create_temporary_system_function("hashCode", hash_code)
t_env.sql_query("SELECT myField, hashCode(myField) FROM MyTable")
```

## 测试自定义函数

假如你定义了如下 Python 自定义函数：

```python
add = udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())
```

如果要对它进行单元测试，首先需要通过 `._func` 从 UDF 对象中抽取原来的 Python 函数，然后才能测试：

```python
f = add._func
assert f(1, 2) == 3
```

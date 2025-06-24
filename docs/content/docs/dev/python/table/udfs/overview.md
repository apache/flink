---
title: "Overview"
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

## Bundling UDFs

To run Python UDFs (as well as Pandas UDFs) in any non-local mode, it is strongly recommended
bundling your Python UDF definitions using the config option [`python-files`]({{< ref "docs/dev/python/python_config" >}}#python-files),
if your Python UDFs live outside the file where the `main()` function is defined.
Otherwise, you may run into `ModuleNotFoundError: No module named 'my_udf'`
if you define Python UDFs in a file called `my_udf.py`.

## Loading resources in UDFs

There are scenarios when you want to load some resources in UDFs first, then running computation
(i.e., `eval`) over and over again, without having to re-load the resources.
For example, you may want to load a large deep learning model only once,
then run batch prediction against the model multiple times.

Overriding the `open` method of `UserDefinedFunction` is exactly what you need.

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

## Accessing job parameters

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

## Testing User-Defined Functions

Suppose you have defined a Python user-defined function as following:

```python
add = udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())
```

To unit test it, you need to extract the original Python function using `._func` and then unit test it:

```python
f = add._func
assert f(1, 2) == 3
```

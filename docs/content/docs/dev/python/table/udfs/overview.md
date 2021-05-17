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

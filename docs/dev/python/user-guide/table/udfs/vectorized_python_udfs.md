---
title: "Vectorized User-defined Functions"
nav-id: vectorized_python_udf
nav-parent_id: python_udf
nav-pos: 30
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

Vectorized Python user-defined functions are functions which are executed by transferring a batch of elements between JVM and Python VM in Arrow columnar format.
The performance of vectorized Python user-defined functions are usually much higher than non-vectorized Python user-defined functions as the serialization/deserialization
overhead and invocation overhead are much reduced. Besides, users could leverage the popular Python libraries such as Pandas, Numpy, etc for the vectorized Python user-defined functions implementation.
These Python libraries are highly optimized and provide high-performance data structures and functions. It shares the similar way as the
[non-vectorized user-defined functions]({% link dev/python/user-guide/table/udfs/python_udfs.md %}) on how to define vectorized user-defined functions.
Users only need to add an extra parameter `udf_type="pandas"` in the decorator `udf` to mark it as a vectorized user-defined function.

**NOTE:** Python UDF execution requires Python version (3.5, 3.6 or 3.7) with PyFlink installed. It's required on both the client side and the cluster side. 

* This will be replaced by the TOC
{:toc}

## Vectorized Scalar Functions

Vectorized Python scalar functions take `pandas.Series` as the inputs and return a `pandas.Series` of the same length as the output.
Internally, Flink will split the input elements into batches, convert a batch of input elements into `Pandas.Series`
and then call user-defined vectorized Python scalar functions for each batch of input elements. Please refer to the config option
[python.fn-execution.arrow.batch.size]({% link dev/python/user-guide/table/python_config.md %}#python-fn-execution-arrow-batch-size) for more details
on how to configure the batch size.

Vectorized Python scalar function could be used in any places where non-vectorized Python scalar functions could be used.

The following example shows how to define your own vectorized Python scalar function which computes the sum of two columns,
and use it in a query:

{% highlight python %}
@udf(result_type=DataTypes.BIGINT(), udf_type="pandas")
def add(i, j):
  return i + j

table_env = BatchTableEnvironment.create(env)

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# register the vectorized Python scalar function
table_env.register_function("add", add)

# use the vectorized Python scalar function in Python Table API
my_table.select("add(bigint, bigint)")

# use the vectorized Python scalar function in SQL API
table_env.sql_query("SELECT add(bigint, bigint) FROM MyTable")
{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

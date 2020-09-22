---
title: "算子"
nav-parent_id: python_datastream_api
nav-pos: 20
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

用户通过算子能将一个或多个 DataStream 转换成新的 DataStream，在应用程序中可以将多个数据转换算子合并成一个复杂的数据流拓扑。

<a name="datastream-transformations"/>

# 数据流转换

Python Flink DataStream 程序主要是通过实现各种算子完成对 DataStream 数据的转换（如 mapping，filtering， reducing等等）。请访问
[算子]({% link dev/stream/operators/index.zh.md %}?code_tab=python)页了解 Python DataStream API 目前已支持的各种算子。

<a name="functions"/>

# 算子函数接口
在 Python DataStream API 中，大部分的算子需要用户实现自定义函数作为算子接口的输入，接下来的内容将描述几种实现自定义函数的方式：

<a name="implementing-function-interfaces"/>

## 实现方法接口
Python DataStream API 给各种算子提供了函数接口，用户可以定义接口的实现，并作为参数传递给算子。例如， `MapFunction` 对应 `map` 转换算子，
`FilterFunction` 对应 `filter` 算子等等。下面以 MapFunction 接口为例：
<p>
{% highlight python %}
# Implementing MapFunction
class MyMapFunction(MapFunction):
    
    def map(value):
        return value + 1
        
data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
mapped_stream = data_stream.map(MyMapFunction(), output_type=Types.INT())
{% endhighlight %}
</p>
<span class="label label-info">注意</span> 在 Python DataStream API, 用户能指定自定义函数的输出数据类型，如若不指定，将默认使用
`Types.PICKLED_BYTE_ARRAY` 数据类型，数据通过 pickle 序列化成字节数组发往下游算子。更多细节请访问
[数据类型]({% link dev/python/datastream-api-users-guide/data_types.zh.md%})页.

<a name="lambda-functions"/>

## Lambda 函数
如此前的例子中所示，大部分算子 API 也能以 lambda 函数的形式定义具体的数据转换逻辑：
<p>
{% highlight python %}
data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
mapped_stream = data_stream.map(lambda x: x + 1, output_type=Types.INT())
{% endhighlight %}
</p>
<span class="label label-info">注意</span> ConnectedStream.map() 和 ConnectedStream.flat_map() 算子目前只支持实现
CoMapFunction 接口和 CoFlatMapFunction 接口。

<a name="python-function"/>

## Python 函数
用户也可以直接通过实现一个 Python 函数来定义具体的数据转换逻辑并传递给算子 API ：
<p>
{% highlight python %}
def my_map_func(value):
    return value + 1

data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
mapped_stream = data_stream.map(my_map_func, output_type=Types.INT())
{% endhighlight %}
</p> 

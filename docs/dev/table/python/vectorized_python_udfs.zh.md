---
title: "自定义向量化函数"
nav-parent_id: python_tableapi
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

向量化的Python用户定义函数是通过以Arrow列格式在JVM和Python VM之间传输一批元素来执行的函数。
矢量化的Python用户定义函数的性能通常比非矢量化的Python用户定义函数要高得多，因为
可以大大减少序列化/反序列化的开销和调用开销。此外，用户可以利用流行的Python库（例如Pandas，Numpy等）来实现矢量化Python用户定义函数的实现。
这些Python库经过高度优化，并提供高性能的数据结构和功能。在如何定义矢量化用户定义函数方面，它与[非矢量化用户定义函数]({{ site.baseurl }}/dev/table/python/python_udfs.html)具有相似的方式。
用户只需要在调用`` udf ``装饰器时添加一个额外的参数`` udf_type="pandas" ``将其标记为一个矢量化用户定义的函数。 

 **注意：**要执行Python UDF，需要安装PyFlink的Python版本（3.5、3.6或3.7）。客户端和群集端都需要它。 

* This will be replaced by the TOC
{:toc}

## 向量化标量函数

向量化的Python标量函数将`` pandas.Series ``作为输入，并返回与输出长度相同的`` pandas.Series `` 。
在内部，Flink会将输入元素拆分为批次，将一批输入元素转换为`` Pandas.Series `` ，
然后为每一批输入元素调用用户定义的矢量化Python标量函数。请参阅配置选项
[python.fn-execution.arrow.batch.size，]({{ site.baseurl }}/dev/table/python/python_config.html#python-fn-execution-arrow-batch-size)
以获取有关如何配置批处理大小的更多详细信息。 

向量化的Python标量函数可以在任何可以使用非向量化的Python标量函数的地方使用。 

以下示例显示了如何定义自己的矢量化Python标量函数，该函数计算两列的总和，
并在查询中使用它： 

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

# 在SQL API中使用Python向量化标量函数
table_env.sql_query("SELECT add(bigint, bigint) FROM MyTable")
{% endhighlight %}

 <span class="label label-info">注意</span>如果不使用RocksDB作为状态后端，则还可以通过
 将**python.fn-execution.memory.managed**设置为**true** ，来配置python worker以使用taskmanager的托管内存。
之后无需设置配置**taskmanager.memory.task.off-heap.size** 。 


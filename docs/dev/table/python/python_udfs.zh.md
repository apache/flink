---
title: "自定义函数（UDF）"
nav-parent_id: python_tableapi
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

用户定义函数是重要的功能，因为它们极大地扩展了Python Table API程序的表达能力。

**NOTE:** 要执行Python UDF，需要安装Python版本3.5、3.6或3.7，并安装PyFlink。 客户端和集群端都需要它。

* This will be replaced by the TOC
{:toc}

## 标量函数（ScalarFunction）

它支持在Python Table API程序中使用Python标量函数。 如果要定义Python标量函数，
可以在`pyflink.table.udf`中扩展基类ScalarFunction，并实现一种评估函数。
Python标量函数的行为由名为`eval`的计算函数定义。
计算函数可以支持可变参数，例如`eval(* args)`。

以下示例显示了如何定义自己的Python哈希码函数，如何在TableEnvironment中注册它以及如何在查询中调用它。
请注意，可以在注册构造函数之前通过构造函数配置标量函数：

{% highlight python %}
class HashCode(ScalarFunction):
  def __init__(self):
    self.factor = 12

  def eval(self, s):
    return hash(s) * self.factor

table_env = BatchTableEnvironment.create(env)

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# 注册Python函数
table_env.register_function("hash_code", udf(HashCode(), DataTypes.BIGINT(), DataTypes.BIGINT()))

# 在Python Table API中使用Python函数
my_table.select("string, bigint, bigint.hash_code(), hash_code(bigint)")

# 在SQL API中使用Python函数
table_env.sql_query("SELECT string, bigint, hash_code(bigint) FROM MyTable")
{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

它还支持在Python Table API程序中使用Java / Scala标量函数。

{% highlight python %}
'''
Java code:

// Java类必须具有公共的无参数构造函数，并且可以在当前的Java类加载器中创建。
public class HashCode extends ScalarFunction {
  private int factor = 12;

  public int eval(String s) {
      return s.hashCode() * factor;
  }
}
'''

table_env = BatchTableEnvironment.create(env)

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# 注册Java函数
table_env.register_java_function("hash_code", "my.java.function.HashCode")

# 在Python Table API中使用Java函数
my_table.select("string.hash_code(), hash_code(string)")

# 在SQL API中使用Java函数
table_env.sql_query("SELECT string, bigint, hash_code(string) FROM MyTable")
{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

除了扩展基类`ScalarFunction`之外，还有许多函数可以定义Python标量函数。
以下示例显示了定义Python标量函数的不同函数，该函数需要两列(column) 
bigint作为输入参数，并返回它们的总和作为结果。

{% highlight python %}
# 选项1：扩展基类`ScalarFunction`
class Add(ScalarFunction):
  def eval(self, i, j):
    return i + j

add = udf(Add(), [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

# 选项2：Python函数
@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add(i, j):
  return i + j

# 选项3：lambda函数
add = udf(lambda i, j: i + j, [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

# 选项4：可调用函数
class CallableAdd(object):
  def __call__(self, i, j):
    return i + j

add = udf(CallableAdd(), [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

# 选项5：部分（partial）函数
def partial_add(i, j, k):
  return i + j + k

add = udf(functools.partial(partial_add, k=1), [DataTypes.BIGINT(), DataTypes.BIGINT()],
          DataTypes.BIGINT())

# 注册Python函数
table_env.register_function("add", add)
# 使用Python Table API中的函数
my_table.select("add(a, b)")
{% endhighlight %}

## Table函数
类似于Python用户定义的标量函数，用户定义的表函数采用零，一或
多个标量值作为输入参数。 但是，与标量函数不同的是，它可以返回
任意数量的行(row)作为输出而不是单个值。 Python UDTF（用户定义的Table函数）的返回类型
可以是Iterable，Iterator或generator的类型。

<!-- TODO: confirm that the following notice is deprecated  -->
<!-- <span class="label label-info">注意</span> 当前，旧计划器（planner)在流和批处理模式下都支持Python UDTF，而在Blink计划器中，仅在流模式下才支持Python UDTF。 -->

以下示例说明了如何定义自己的Python多发出函数，并将其注册到
TableEnvironment，并在查询中调用它。

{% highlight python %}
class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# 注册Python Table函数
table_env.register_function("split", udtf(Split(), DataTypes.STRING(), [DataTypes.STRING(), DataTypes.INT()]))

# 在Python Table API中使用Python Table函数
my_table.join_lateral("split(a) as (word, length)")
my_table.left_outer_join_lateral("split(a) as (word, length)")

# 在SQL API中使用Python Table函数
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")

{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

它还支持在Python Table API程序中使用Java / Scala表函数。
{% highlight python %}
'''
Java code:

// 通用类型"Tuple2 <String，Integer>"将返回表的模式确定为（String，Integer）。
// Java类必须具有公共的无参数构造函数，并且可以在当前的Java类加载器中创建。
public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";
    
    public void eval(String str) {
        for (String s : str.split(separator)) {
            // use collect(...) to emit a row
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}
'''

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# 注册java函数。
table_env.register_java_function("split", "my.java.function.Split")

# 使用Python Table API中的table函数。 "as"指定表的字段名称。
my_table.join_lateral("split(a) as (word, length)").select("a, word, length")
my_table.left_outer_join_lateral("split(a) as (word, length)").select("a, word, length")

# 注册python函数。

# 在SQL中将table函数与LATERAL和TABLE关键字一起使用。
# CROSS JOIN表函数（等效于Table API中的"join"）。
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
# LEFT JOIN一个表函数（等同于Table API中的"left_outer_join"）。
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")
{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

像Python标量函数一样，您可以使用上述五种方式来定义Python TableFunction。

<span class="label label-info">注意</span> 唯一的区别是Python Table Functions的返回类型必须是iterable（可迭代子类）, iterator（迭代器） or generator（生成器）。

{% highlight python %}
# 选项1：生成器函数
@udtf(input_types=DataTypes.BIGINT(), result_types=DataTypes.BIGINT())
def generator_func(x):
      yield 1
      yield 2

# 选项2：返回迭代器
@udtf(input_types=DataTypes.BIGINT(), result_types=DataTypes.BIGINT())
def iterator_func(x):
      return range(5)

# 选项3：返回可迭代子类
@udtf(input_types=DataTypes.BIGINT(), result_types=DataTypes.BIGINT())
def iterable_func(x):
      result = [1, 2, 3]
      return result

table_env.register_function("iterable_func", iterable_func)
table_env.register_function("iterator_func", iterator_func)
table_env.register_function("generator_func", generator_func)

{% endhighlight %}

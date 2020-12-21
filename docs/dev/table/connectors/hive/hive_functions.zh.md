---
title: "Hive 函数"
nav-parent_id: hive_tableapi
nav-pos: 4
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

## 通过 HiveModule 使用 Hive 内置函数

在 Flink SQL 和 Table API 中，可以通过系统内置的 `HiveModule` 来使用 Hive 内置函数，

详细信息，请参考 [HiveModule]({% link dev/table/modules.zh.md %}#hivemodule)。

<div class="codetabs" markdown="1">
<div data-lang="Java" markdown="1">
{% highlight java %}

String name            = "myhive";
String version         = "2.3.4";

tableEnv.loadModue(name, new HiveModule(version));
{% endhighlight %}
</div>
<div data-lang="Scala" markdown="1">
{% highlight scala %}

val name            = "myhive"
val version         = "2.3.4"

tableEnv.loadModue(name, new HiveModule(version));
{% endhighlight %}
</div>
<div data-lang="Python" markdown="1">
{% highlight Python %}
from pyflink.table.module import HiveModule

name = "myhive"
version = "2.3.4"

t_env.load_module(name, HiveModule(version))
{% endhighlight %}
</div>
<div data-lang="YAML" markdown="1">
{% highlight yaml %}
modules:
   - name: core
     type: core
   - name: myhive
     type: hive
{% endhighlight %}
</div>
</div>

* 请注意旧版本的部分 Hive 内置函数存在[线程安全问题](https://issues.apache.org/jira/browse/HIVE-16183)。
我们建议用户及时通过补丁修正 Hive 中的这些问题。

## Hive 用户自定义函数(User Defined Functions)

在 Flink 中用户可以使用 Hive 里已经存在的 UDF 函数。

支持的 UDF 类型包括：

- UDF
- GenericUDF
- GenericUDTF
- UDAF
- GenericUDAFResolver2

在进行查询规划和执行时，Hive UDF 和 GenericUDF 函数会自动转换成 Flink 中的 ScalarFunction，GenericUDTF 会被自动转换成 Flink 中的
 TableFunction，UDAF 和 GenericUDAFResolver2 则转换成 Flink 聚合函数(AggregateFunction).

想要使用 Hive UDF 函数，需要如下几步：

- 通过 Hive Metastore 将带有 UDF 的 HiveCatalog 设置为当前会话的 catalog 后端。
- 将带有 UDF 的 jar 包放入 Flink classpath 中，并在代码中引入。
- 使用 Blink planner。

## 使用 Hive UDF

假设我们在 Hive Metastore 中已经注册了下面的 UDF 函数：


{% highlight java %}
/**
 * 注册为 'myudf' 的简单 UDF 测试类. 
 */
public class TestHiveSimpleUDF extends UDF {

	public IntWritable evaluate(IntWritable i) {
		return new IntWritable(i.get());
	}

	public Text evaluate(Text text) {
		return new Text(text.toString());
	}
}

/**
 * 注册为 'mygenericudf' 的普通 UDF 测试类
 */
public class TestHiveGenericUDF extends GenericUDF {

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		checkArgument(arguments.length == 2);

		checkArgument(arguments[1] instanceof ConstantObjectInspector);
		Object constant = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue();
		checkArgument(constant instanceof IntWritable);
		checkArgument(((IntWritable) constant).get() == 1);

		if (arguments[0] instanceof IntObjectInspector ||
				arguments[0] instanceof StringObjectInspector) {
			return arguments[0];
		} else {
			throw new RuntimeException("Not support argument: " + arguments[0]);
		}
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		return arguments[0].get();
	}

	@Override
	public String getDisplayString(String[] children) {
		return "TestHiveGenericUDF";
	}
}

/**
 * 注册为 'mygenericudtf' 的字符串分割 UDF 测试类
 */
public class TestHiveUDTF extends GenericUDTF {

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		checkArgument(argOIs.length == 2);

		// TEST for constant arguments
		checkArgument(argOIs[1] instanceof ConstantObjectInspector);
		Object constant = ((ConstantObjectInspector) argOIs[1]).getWritableConstantValue();
		checkArgument(constant instanceof IntWritable);
		checkArgument(((IntWritable) constant).get() == 1);

		return ObjectInspectorFactory.getStandardStructObjectInspector(
			Collections.singletonList("col1"),
			Collections.singletonList(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
	}

	@Override
	public void process(Object[] args) throws HiveException {
		String str = (String) args[0];
		for (String s : str.split(",")) {
			forward(s);
			forward(s);
		}
	}

	@Override
	public void close() {
	}
}

{% endhighlight %}

在 Hive CLI 中，可以查询到已经注册的 UDF 函数:

{% highlight bash %}
hive> show functions;
OK
......
mygenericudf
myudf
myudtf

{% endhighlight %}

此时，用户如果想使用这些 UDF，在 SQL 中就可以这样写：


{% highlight bash %}

Flink SQL> select mygenericudf(myudf(name), 1) as a, mygenericudf(myudf(age), 1) as b, s from mysourcetable, lateral table(myudtf(name, 1)) as T(s);

{% endhighlight %}

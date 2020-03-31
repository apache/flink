---
title: "Hive 函数"
nav-parent_id: hive_tableapi
nav-pos: 3
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

`HiveModule` 为 Flink SQL 和 Table API 用户提供了 Hive 内置函数，作为 Flink 系统（内置）函数。

有关详细信息，请参阅 [HiveModule]({{ site.baseurl }}/zh/dev/table/modules.html#hivemodule).

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

* 注意，旧版本中的某些 Hive 内置函数具有[线程安全问题](https://issues.apache.org/jira/browse/HIVE-16183)。我们建议用户对自己的 Hive 来打补丁修复它们。

## Hive 用户定义函数（User Defined Functions）

用户可以在 Flink 中使用其现有的 Hive 用户定义函数。

支持的UDF类型包括：

- UDF
- GenericUDF
- GenericUDTF
- UDAF
- GenericUDAFResolver2

在查询计划和执行时，Hive 的 UDF 和 GenericUDF 会自动转换为 Flink 的 ScalarFunction，Hive 的 GenericUDTF 会自动转换为 Flink 的 TableFunction，Hive 的 UDAF 和 GenericUDAFResolver2 会转换为 Flink 的 AggregateFunction。

要使用 Hive 用户定义函数，用户必须

- 设置由 Hive Metastore 支持的 HiveCatalog，其中包含该函数，且作为会话（session）的当前 catalog
- 在 Flink 的 classpath 中设置一个包含该函数的 jar 文件
- 使用 Blink planner

## 使用 Hive 用户定义函数

假设我们在 Hive Metastore 中注册了以下 Hive 函数：

{% highlight java %}
/**
 * Test simple udf. Registered under name 'myudf'
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
 * Test generic udf. Registered under name 'mygenericudf'
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
 * Test split udtf. Registered under name 'mygenericudtf'
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

在 Hive CLI 中，我们可以看到它们已注册：

{% highlight bash %}
hive> show functions;
OK
......
mygenericudf
myudf
myudtf

{% endhighlight %}

然后，用户可以在 SQL 中以如下方式使用它们：

{% highlight bash %}

Flink SQL> select mygenericudf(myudf(name), 1) as a, mygenericudf(myudf(age), 1) as b, s from mysourcetable, lateral table(myudtf(name, 1)) as T(s);

{% endhighlight %}

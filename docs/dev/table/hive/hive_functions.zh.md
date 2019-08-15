---
title: "Hive Functions"
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

## Hive User Defined Functions

Users can use their existing Hive User Defined Functions in Flink.

Supported UDF types include:

- UDF
- GenericUDF
- GenericUDTF
- UDAF
- GenericUDAFResolver2

Upon query planning and execution, Hive's UDF and GenericUDF are automatically translated into Flink's ScalarFunction,
Hive's GenericUDTF is automatically translated into Flink's TableFunction,
and Hive's UDAF and GenericUDAFResolver2 are translated into Flink's AggregateFunction.

To use a Hive User Defined Function, user have to

- set a HiveCatalog backed by Hive Metastore that contains that function as current catalog of the session
- include a jar that contains that function in Flink's classpath
- use Blink planner.

## Using Hive User Defined Functions

Assuming we have the following Hive functions registered in Hive Metastore:


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

From Hive CLI, we can see they are registered:

{% highlight bash %}
hive> show functions;
OK
......
mygenericudf
myudf
myudtf

{% endhighlight %}


Then, users can use them in SQL as:


{% highlight bash %}

Flink SQL> select mygenericudf(myudf(name), 1) as a, mygenericudf(myudf(age), 1) as b, s from mysourcetable, lateral table(myudtf(name, 1)) as T(s);

{% endhighlight %}


### Limitations

Hive built-in functions are currently not supported out of box in Flink. To use Hive built-in functions, users must register them manually in Hive Metastore first.

Support for Hive functions has only been tested for Flink batch in Blink planner.

Hive functions currently cannot be used across catalogs in Flink.

Please reference to [Hive]({{ site.baseurl }}/dev/table/hive/index.html) for data type limitations.

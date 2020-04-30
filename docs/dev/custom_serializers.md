---
title: Register a custom serializer for your Flink program
nav-title: Custom Serializers
nav-parent_id: types
nav-pos: 10
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

If you use a custom type in your Flink program which cannot be serialized by the
Flink type serializer, Flink falls back to using the generic Kryo
serializer. You may register your own serializer or a serialization system like
Google Protobuf or Apache Thrift with Kryo. To do that, simply register the type
class and the serializer in the `ExecutionConfig` of your Flink program.


{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// register the class of the serializer as serializer for a type
env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, MyCustomSerializer.class);

// register an instance as serializer for a type
MySerializer mySerializer = new MySerializer();
env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, mySerializer);
{% endhighlight %}

Note that your custom serializer has to extend Kryo's Serializer class. In the
case of Google Protobuf or Apache Thrift, this has already been done for
you:

{% highlight java %}

final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// register the Google Protobuf serializer with Kryo
env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, ProtobufSerializer.class);

// register the serializer included with Apache Thrift as the standard serializer
// TBaseSerializer states it should be initialized as a default Kryo serializer
env.getConfig().addDefaultKryoSerializer(MyCustomType.class, TBaseSerializer.class);

{% endhighlight %}

For the above example to work, you need to include the necessary dependencies in
your Maven project file (pom.xml). In the dependency section, add the following
for Apache Thrift:

{% highlight xml %}

<dependency>
	<groupId>com.twitter</groupId>
	<artifactId>chill-thrift</artifactId>
	<version>0.7.6</version>
	<!-- exclusions for dependency conversion -->
	<exclusions>
		<exclusion>
			<groupId>com.esotericsoftware.kryo</groupId>
			<artifactId>kryo</artifactId>
		</exclusion>
	</exclusions>
</dependency>
<!-- libthrift is required by chill-thrift -->
<dependency>
	<groupId>org.apache.thrift</groupId>
	<artifactId>libthrift</artifactId>
	<version>0.11.0</version>
	<exclusions>
		<exclusion>
			<groupId>javax.servlet</groupId>
			<artifactId>servlet-api</artifactId>
		</exclusion>
		<exclusion>
			<groupId>org.apache.httpcomponents</groupId>
			<artifactId>httpclient</artifactId>
		</exclusion>
	</exclusions>
</dependency>

{% endhighlight %}

For Google Protobuf you need the following Maven dependency:

{% highlight xml %}

<dependency>
	<groupId>com.twitter</groupId>
	<artifactId>chill-protobuf</artifactId>
	<version>0.7.6</version>
	<!-- exclusions for dependency conversion -->
	<exclusions>
		<exclusion>
			<groupId>com.esotericsoftware.kryo</groupId>
			<artifactId>kryo</artifactId>
		</exclusion>
	</exclusions>
</dependency>
<!-- We need protobuf for chill-protobuf -->
<dependency>
	<groupId>com.google.protobuf</groupId>
	<artifactId>protobuf-java</artifactId>
	<version>3.7.0</version>
</dependency>

{% endhighlight %}


Please adjust the versions of both libraries as needed.

### Issue with using Kryo's `JavaSerializer` 

If you register Kryo's `JavaSerializer` for your custom type, you may
encounter `ClassNotFoundException`s even though your custom type class is
included in the submitted user code jar. This is due to a know issue with
Kryo's `JavaSerializer`, which may incorrectly use the wrong classloader.

In this case, you should use `org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer`
instead to resolve the issue. This is a reimplemented `JavaSerializer` in Flink
that makes sure the user code classloader is used.

Please refer to [FLINK-6025](https://issues.apache.org/jira/browse/FLINK-6025)
for more details.

{% top %}

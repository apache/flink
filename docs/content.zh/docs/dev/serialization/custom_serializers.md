---
title: 自定义序列化器
weight: 11
type: docs
aliases:
  - /zh/dev/custom_serializers.html
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

# 为你的 Flink 程序注册自定义序列化器

如果在 Flink 程序中使用了 Flink 类型序列化器无法进行序列化的用户自定义类型，Flink 会回退到通用的 Kryo 序列化器。
可以使用 Kryo 注册自己的序列化器或序列化系统，比如 Google Protobuf 或 Apache Thrift。
使用方法是在 Flink 程序中的 `ExecutionConfig` 注册类类型以及序列化器。

```java
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// 为类型注册序列化器类
env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, MyCustomSerializer.class);

// 为类型注册序列化器实例
MySerializer mySerializer = new MySerializer();
env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, mySerializer);
```

需要确保你的自定义序列化器继承了 Kryo 的序列化器类。
对于 Google Protobuf 或 Apache Thrift，这一点已经为你做好了：

```java

final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// 使用 Kryo 注册 Google Protobuf 序列化器
env.getConfig().registerTypeWithKryoSerializer(MyCustomType.class, ProtobufSerializer.class);

// 注册 Apache Thrift 序列化器为标准序列化器
// TBaseSerializer 需要初始化为默认的 kryo 序列化器
env.getConfig().addDefaultKryoSerializer(MyCustomType.class, TBaseSerializer.class);

```

为了使上面的例子正常工作，需要在 Maven 项目文件中（pom.xml）包含必要的依赖。
为 Apache Thrift 添加以下依赖：

```xml

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

```

对于 Google Protobuf 需要添加以下 Maven 依赖：

```xml

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

```


请根据需要调整两个依赖库的版本。

### 使用 Kryo `JavaSerializer` 的问题

如果你为自定义类型注册 Kryo 的 `JavaSerializer`，即使你提交的 jar 中包含了自定义类型的类，也可能会遇到 `ClassNotFoundException` 异常。
这是由于 Kryo `JavaSerializer` 的一个已知问题，它可能使用了错误的类加载器。

在这种情况下，你应该使用 `org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer` 来解决这个问题。
这个类是在 Flink 中对 JavaSerializer 的重新实现，可以确保使用用户代码的类加载器。

更多细节可以参考 [FLINK-6025](https://issues.apache.org/jira/browse/FLINK-6025)。

{{< top >}}

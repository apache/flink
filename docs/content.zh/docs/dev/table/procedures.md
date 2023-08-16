---
title: "存储过程"
is_beta: true
weight: 50
type: docs
aliases:
  - /zh/dev/table/procedures.html
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

# 存储过程


Flink 允许用户在 Table API 和 SQL 中调用存储过程来完成一些特定任务，比如处理数据，数据管理类任务等。存储过程可以通过 `StreamExecutionEnvironment` 来运行 Flink 作业，这使得存储过程更加强大和灵活。

## 开发指南
------------------

为了调用一个存储过程，需要确保一个 Catalog 可以提供这个存储过程。为了让一个 Catalog 提供存储过程，你首先需要实现一个存储过程，然后在方法 `Catalog.getProcedure(ObjectPath procedurePath)` 返回这个存储过程。
下面的步骤将展示如何实现一个存储过程并让一个 Catalog 提供这个存储过程。

### 存储过程类

存储过程的实现类必须实现接口 `org.apache.flink.table.procedures.Procedure`。

该实现类必须声明为 `public`， 而不是 `abstract`， 并且可以被全局访问。不允许使用非静态内部类或匿名类。

### Call 方法

存储过程的接口不提供任何方法，存储过程的实现类必须有名为 `call` 的方法，在该方法里面可以实现存储过程实际的逻辑。`call` 方法必须被声明为 `public`， 并且带有一组定义明确的参数。

请注意:

* `call` 方法的第一个参数总是应该为 `ProcedureContext`，该参数提供了方法 `getExecutionEnvironment()` 来得到当前的 `StreamExecutionEnvironment`。通过 `StreamExecutionEnvironment` 可以运行一个 Flink 作业；
* `call` 方法的返回类型应该永远都是一个数组类型，比如 `int[]`，`String[]`，等等；

更多的细节请参考类 `org.apache.flink.table.procedures.Procedure` 的 Java 文档。

常规的 JVM 方法调用语义是适用的，因此可以：
- 实现重载的方法，例如 `call(ProcedureContext, Integer)` and `call(ProcedureContext, LocalDateTime)`；
- 使用变长参数，例如 `call(ProcedureContext, Integer...)`；
- 使用对象继承，例如 `call(ProcedureContext, Object)` 可接受 `LocalDateTime` 和 `Integer` 作为参数；
- 也可组合使用，例如 `call(ProcedureContext, Object...)` 可接受所有类型的参数；

如果你希望用 Scala 来实现一个存储过程，对应可变长参数的情况，请添加 `scala.annotation.varargs`。另外，推荐使用装箱的基本类型（比如，使用 `java.lang.Integer` 而不是 `Int`）来支持 `NULL`。

下面的代码片段展示来一个重载存储过程的例子：

{{< tabs "7c5a5392-30d7-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;

// 有多个重载 call 方法的存储过程
public class GenerateSequenceProcedure implements Procedure {

  public long[] call(ProcedureContext context, int n) {
    return generate(context.getExecutionEnvironment(), n);
  }

  public long[] call(ProcedureContext context, String n) {
    return generate(context.getExecutionEnvironment(), Integer.parseInt(n));
  }

  private long[] generate(StreamExecutionEnvironment env, int n) throws Exception {
    long[] sequenceN = new long[n];
    int i = 0;
    try (CloseableIterator<Long> result = env.fromSequence(0, n - 1).executeAndCollect()) {
      while (result.hasNext()) {
        sequenceN[i++] = result.next();
      }
    }
    return sequenceN;
  }
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.table.procedure.ProcedureContext
import org.apache.flink.table.procedures.Procedure
import scala.annotation.varargs

// 有多个重载 call 方法的存储过程
class GenerateSequenceProcedure extends Procedure {

  def call(context: ProcedureContext, a: Integer, b: Integer): Array[Integer] = {
    Array(a + b)
  }

  def call(context: ProcedureContext, a: String, b: String): Array[Integer] = {
    Array(Integer.valueOf(a) + Integer.valueOf(b))
  }

  @varargs // 类似 Java 的变长参数
  def call(context: ProcedureContext, d: Double*): Array[Integer] = {
    Array(d.sum.toInt)
  }
}

```
{{< /tab >}}
{{< /tabs >}}

### 类型推导
Table（类似于 SQL 标准）是一种强类型的 API。 因此，存储过程的参数和返回类型都必须映射到 [data type]({{< ref "docs/dev/table/types" >}})。

从逻辑角度看，Planner 需要知道数据类型、精度和小数位数；从 JVM 角度来看，Planner 在调用存储过程时需要知道如何将内部数据结构表示为 JVM 对象。

术语 _类型推导_ 概括了意在验证输入值、推导出参数/返回值数据类型的逻辑。

Flink 存储过程实现了自动的类型推导提取，通过反射从存储过程的类及其 `call` 方法中推导数据类型。如果这种隐式的反射提取方法不成功，则可以通过使用 `@DataTypeHint` 和 `@ProcedureHint` 注解相关参数、类或方法来支持提取存储过程的参数和返回类型，下面展示了有关如何注解存储过程的例子。

需要注意的是虽然存储过程的 `call` 方法必须返回数组类型 `T[]`，但是如果用 `@DataTypeHint` 来注解返回类型，实际上注解的是该数组的元素的类型，即 `T`。

#### 自动类型推导 

自动类型推导会检查存储过程的类和 `call` 方法，推导出存储过程参数和结果的数据类型， `@DataTypeHint` 和 `@ProcedurenHint` 注解支持自动类型推导。

有关可以隐式映射到数据类型的类的完整列表, 请参阅[data type extraction section]({{< ref "docs/dev/table/types" >}}#data-type-extraction)。

**`@DataTypeHint`**

在许多情况下，需要支持以 _内联_ 方式自动提取出存储过程参数、返回值的类型。

以下例子展示了如何使用 `@DataTypeHint`，详情可参考该注解类的文档。

{{< tabs "81b297da-30d9-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.procedure.ProcedureContext
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.types.Row;

// 有多个重载 call 方法的存储过程
public static class OverloadedProcedure implements Procedure {

  // 不需要 hint
  public Long[] call(ProcedureContext context, long a, long b) {
    return new Long[] {a + b};
  }

  // 定义 decimal 的精度和小数位
  public @DataTypeHint("DECIMAL(12, 3)") BigDecimal[] call(ProcedureContext context, double a, double b) {
    return new BigDecimal[] {BigDecimal.valueOf(a + b)};
  }

  // 定义嵌套数据类型
  @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
  public Row[] call(ProcedureContext context, int i) {
    return new Row[] {Row.of(String.valueOf(i), Instant.ofEpochSecond(i))};
  }

  // 允许任意类型的输入，并输出序列化定制后的值
  @DataTypeHint(value = "RAW", bridgedTo = ByteBuffer.class)
  public ByteBuffer[] call(ProcedureContext context, @DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
    return new ByteBuffer[] {MyUtils.serializeToByteBuffer(o)};
  }
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.InputGroup
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import scala.annotation.varargs

import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.InputGroup
import org.apache.flink.table.procedure.ProcedureContext
import org.apache.flink.table.procedures.Procedure
import org.apache.flink.types.Row

// 有多个重载 call 方法的存储过程
class OverloadedProcedure extends Procedure {

  // 不需要 hint
  def call(context: ProcedureContext, a: Long, b: Long): Array[Long] = {
    Array(a + b)
  }

  // 定义 decimal 的精度和小数位
  @DataTypeHint("DECIMAL(12, 3)")
  def call(context: ProcedureContext, a: Double, b: Double): Array[BigDecimal] = {
    Array(BigDecimal.valueOf(a + b))
  }

  // 定义嵌套数据类型
  @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
  def call(context: ProcedureContext, i: Integer): Array[Row] = {
    Row.of(java.lang.String.valueOf(i), java.time.Instant.ofEpochSecond(i))
  }

  // 允许任意类型的输入，并输出序列化定制后的值
  @DataTypeHint(value = "RAW", bridgedTo = classOf[java.nio.ByteBuffer])
  def call(context: ProcedureContext, @DataTypeHint(inputGroup = InputGroup.ANY) o: Object): Array[java.nio.ByteBuffer] = {
    Array[MyUtils.serializeToByteBuffer(o)]
  }
}

```
{{< /tab >}}
{{< /tabs >}}

**`@ProcedureHint`**

有时我们希望一种 `call` 方法可以同时处理多种数据类型，有时又要求对重载的多个 `call` 方法仅声明一次通用的返回类型。

`@ProcedureHint` 注解可以提供从入参数据类型到返回数据类型的映射，它可以在整个存储过程类或 `call` 方法上注解输入和返回的数据类型。可以在类顶部声明一个或多个注解，也可以为类的所有 `call` 方法分别声明一个或多个注解。所有的 hint 参数都是可选的，如果未定义参数，则使用默认的基于反射的类型提取。在函数类顶部定义的 hint 参数被所有 `call` 方法继承。

以下例子展示了如何使用 `@ProcedureHint`，详情可参考该注解类的文档。

{{< tabs "5d205654-30da-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.types.Row;

// 为存储过程类的所有 call 方法指定同一个返回类型
@ProcedureHint(output = @DataTypeHint("ROW<s STRING, i INT>"))
public static class OverloadedProcedure implements Procedure {

  public Row[] call(ProcedureContext context, int a, int b) {
    return new Row[] {Row.of("Sum", a + b)};
  }

  // 仍然可以重载 call 方法
  public Row[] call(ProcedureContext context) {
    return new Row[] {Row.of("Empty args", -1)};
  }
}

// 解耦类型推导与 call 方法，类型推导完全取决于 ProcedureHint
@ProcedureHint(
  input = {@DataTypeHint("INT"), @DataTypeHint("INT")},
  output = @DataTypeHint("INT")
)
@ProcedureHint(
  input = {@DataTypeHint("BIGINT"), @DataTypeHint("BIGINT")},
  output = @DataTypeHint("BIGINT")
)
@ProcedureHint(
  input = {},
  output = @DataTypeHint("BOOLEAN")
)
public static class OverloadedProcedure implements Procedure {

  // 一个 call 方法的实现，确保 call 方法存在于存储过程类中，可以被 JVM 调用
  public Object[] call(ProcedureContext context, Object... o) {
    if (o.length == 0) {
      return new Object[] {false};
    }
    return new Object[] {o[0]};
  }
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.ProcedureHint
import org.apache.flink.table.procedure.ProcedureContext
import org.apache.flink.table.procedures.Procedure
import org.apache.flink.types.Row
import scala.annotation.varargs

// 为存储过程类的所有 call 方法指定同一个返回类型
@ProcedureHint(output = new DataTypeHint("ROW<s STRING, i INT>"))
class OverloadedFunction extends Procedure {

  def call(context: ProcedureContext, a: Int, b: Int): Array[Row] = {
    Array(Row.of("Sum", Int.box(a + b)))
  }

  // 仍然可以重载 call 方法
  def call(context: ProcedureContext): Array[Row] = {
    Array(Row.of("Empty args", Int.box(-1)))
  }
}

// 解耦类型推导与 call 方法，类型推导完全取决于 ProcedureHint
@ProcedureHint(
  input = Array(new DataTypeHint("INT"), new DataTypeHint("INT")),
  output = new DataTypeHint("INT")
)
@ProcedureHint(
  input = Array(new DataTypeHint("BIGINT"), new DataTypeHint("BIGINT")),
  output = new DataTypeHint("BIGINT")
)
@ProcedureHint(
  input = Array(),
  output = new DataTypeHint("BOOLEAN")
)
class OverloadedProcedure extends Procedure {

  // 一个 call 方法的实现，确保 call 方法存在于存储过程类中，可以被 JVM 调用
  @varargs
  def call(context: ProcedureContext, o: AnyRef*): Array[AnyRef]= {
    if (o.length == 0) {
      Array(Boolean.box(false))
    }
    Array(o(0))
  }
}

```
{{< /tab >}}
{{< /tabs >}}


### 在 Catalog 中返回存储过程
在实现了一个存储过程后，Catalog 可以通过方法 `Catalog.getProcedure(ObjectPath procedurePath)` 来返回该存储过程，下面的例子展示了如何在 Catalog 中返回存储过程。
另外也可以在 `Catalog.listProcedures(String dbName)` 方法中列出所有的存储过程。

{{< tabs "2ee21ac4-30db-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}
```java

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;

import java.util.HashMap;
import java.util.Map;

// 有内置 procedure 的 Catalog
public class CatalogWithBuiltInProcedure extends GenericInMemoryCatalog {

  static {
    PROCEDURE_MAP.put(ObjectPath.fromString("system.generate_n"), new GenerateSequenceProcedure());
  }

  public CatalogWithBuiltInProcedure(String name) {
    super(name);
  }
  
  @Override
  public List<String> listProcedures(String dbName) throws DatabaseNotExistException, CatalogException {
    return PROCEDURE_MAP.keySet().stream().filter(procedurePath -> procedurePath.getDatabaseName().equals(dbName))
            .map(ObjectPath::getObjectName).collect(Collectors.toList());
  }

  @Override
  public Procedure getProcedure(ObjectPath procedurePath) throws ProcedureNotExistException, CatalogException {
    if (PROCEDURE_MAP.containsKey(procedurePath)) {
      return PROCEDURE_MAP.get(procedurePath);
    } else {
      throw new ProcedureNotExistException(getName(), procedurePath);
    }
  }
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.procedures.Procedure;

// 有内置 procedure 的 Catalog
class CatalogWithBuiltInProcedure(name: String) extends GenericInMemoryCatalog(name) {

  val PROCEDURE_MAP = collection.immutable.HashMap[ObjectPath, Procedure](
    ObjectPath.fromString("system.generate_n"), new GenerateSequenceProcedure());

  @throws(classOf[DatabaseNotExistException])
  @throws(classOf[CatalogException])
  override def listProcedures(dbName: String): List[String] = {
    if (!databaseExists(dbName)) {
      throw new DatabaseNotExistException(getName, dbName);
    }
    PROCEDURE_MAP.keySet.filter(procedurePath => procedurePath.getDatabaseName.equals(dbName))
      .map(procedurePath => procedurePath.getObjectName).toList
  }

  @throws(classOf[ProcedureNotExistException])
  override def getProcedure(procedurePath: ObjectPath): Procedure = {
    if (PROCEDURE_MAP.contains(procedurePath)) {
      PROCEDURE_MAP(procedurePath);
    } else {
      throw new ProcedureNotExistException(getName, procedurePath)
    }
  }
}

```
{{< /tab >}}
{{< /tabs >}}


## 例子

下面的例子展示了如何在一个 Catalog 中提供一个存储过程并且通过 `CALL` 语句来调用这个存储过程。详情可参考[开发指南](#implementation-guide)。

{{< tabs "c3edd888-30db-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}
```java

import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;

// 首先实现一个存储过程
public static class GenerateSequenceProcedure implements Procedure {

  public long[] call(ProcedureContext context, int n) {
    long[] sequenceN = new long[n];
    int i = 0;
    try (CloseableIterator<Long> result = env.fromSequence(0, n - 1).executeAndCollect()) {
      while (result.hasNext()) {
        sequenceN[i++] = result.next();
      }
    }
    return sequenceN;
  }
}

// 自定义一个 Catalog，并返回该存储过程
public static class CatalogWithBuiltInProcedure extends GenericInMemoryCatalog {

  static {
    PROCEDURE_MAP.put(ObjectPath.fromString("system.generate_n"), new GenerateSequenceProcedure());
  }
  // 省略一些方法
  // ...  
  @Override
  public Procedure getProcedure(ObjectPath procedurePath) throws ProcedureNotExistException, CatalogException {
    if (PROCEDURE_MAP.containsKey(procedurePath)) {
      return PROCEDURE_MAP.get(procedurePath);
    } else {
        throw new ProcedureNotExistException(getName(), procedurePath);
    }
  }
}

TableEnvironment tEnv = TableEnvironment.create(...);
// 注册这个 Catalog
tEnv.registerCatalog("my_catalog", new CatalogWithBuiltInProcedure());
// 通过 Call 语句调用该存储过程
tEnv.executeSql("call my_catalog.`system`.generate_n(5)");

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

import org.apache.flink.table.catalog.{GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.catalog.exceptions.{CatalogException, ProcedureNotExistException}
import org.apache.flink.table.procedure.ProcedureContext
import org.apache.flink.table.procedures.Procedure

// 首先实现一个存储过程
class GenerateSequenceProcedure extends Procedure {

  def call(context: ProcedureContext, n: Integer): Array[Long] = {
    val env = context.getExecutionEnvironment
    val sequenceN = Array[Long]
    var i = 0;
    env.fromSequence(0, n - 1).executeAndCollect()
      .forEachRemaining(r => {
        sequenceN(i) = r
        i = i + 1
      })
    sequenceN;
  }
}

// 然后在一个自定义的 catalog 返回该 procedure
class CatalogWithBuiltInProcedure(name: String) extends GenericInMemoryCatalog(name) {

  val PROCEDURE_MAP = collection.immutable.HashMap[ObjectPath, Procedure](ObjectPath.fromString("system.generate_n"),
    new GenerateSequenceProcedure());

  // 省略一些方法
  // ...  

  @throws(classOf[ProcedureNotExistException])
  override def getProcedure(procedurePath: ObjectPath): Procedure = {
    if (PROCEDURE_MAP.contains(procedurePath)) {
      PROCEDURE_MAP(procedurePath);
    } else {
      throw new ProcedureNotExistException(getName, procedurePath)
    }
  }
}

TableEnvironment tEnv = TableEnvironment.create(...)
// 注册该 catalog
tEnv.registerCatalog("my_catalog", new CatalogWithBuiltInProcedure())
// 通过 Call 语句调用该存储过程
tEnv.executeSql("call my_catalog.`system`.generate_n(5)")

```
{{< /tab >}}
{{< /tabs >}}



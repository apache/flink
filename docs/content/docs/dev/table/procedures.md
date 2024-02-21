---
title: "Procedures"
is_beta: true
weight: 50
type: docs
aliases:
  - /dev/table/procedures.html
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

# Procedures

Flink Table API & SQL empowers users to perform data manipulation and administrative tasks with procedures. Procedures can run FLINK jobs with the provided `StreamExecutionEnvironment`, making them more powerful and flexible.

## Implementation Guide

To call a procedure, it must be available in a catalog. To provide procedures in a catalog, you need to implement the procedure and then return it using the `Catalog.getProcedure(ObjectPath procedurePath)` method.
The following steps will guild you on how to implement and provide a procedure in a catalog.

### Procedure Class

An implementation class must implement the interface `org.apache.flink.table.procedures.Procedure`.

The class must be declared `public`, not `abstract`, and should be globally accessible. Thus, non-static inner or anonymous classes are not allowed.

### Call Methods

The interface doesn't provide any method，you have to define a method named `call` in which you can implement the logic of the procedure.
The methods must be declared `public` and take a well-defined set of arguments. 

Please note:

* The first parameter of the method `call` should always be `ProcedureContext` which provides the method `getExecutionEnvironment` to get a `StreamExecutionEnvironment` for running a Flink Job
* The return type should always be an array, like `int[]`, `String[]`, etc

More detail can be found in the Java doc of the class `org.apache.flink.table.procedures.Procedure`.

Regular JVM method calling semantics apply. Therefore, it is possible to:
- implement overloaded methods such as `call(ProcedureContext, Integer)` and `call(ProcedureContext, LocalDateTime)`
- use var-args such as `call(ProcedureContext, Integer...)`
- use object inheritance such as `call(ProcedureContext, Object)` that takes both `LocalDateTime` and `Integer`
- and combinations of the above such as `call(ProcedureContext, Object...)` that takes all kinds of arguments

If you intend to implement procedures in Scala, please add the `scala.annotation.varargs` annotation in
case of variable arguments. Furthermore, it is recommended to use boxed primitives (e.g. `java.lang.Integer`
instead of `Int`) to support `NULL`.

The following snippets shows an example of an overloaded procedure:

{{< tabs "0819d780-3052-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;

// procedure with overloaded call methods
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

// procedures with overloaded call methods
class GenerateSequenceProcedure extends Procedure {

  def call(context: ProcedureContext, a: Integer, b: Integer): Array[Integer] = {
    Array(a + b)
  }

  def call(context: ProcedureContext, a: String, b: String): Array[Integer] = {
    Array(Integer.valueOf(a) + Integer.valueOf(b))
  }

  @varargs // generate var-args like Java
  def call(context: ProcedureContext, d: Double*): Array[Integer] = {
    Array(d.sum.toInt)
  }
}

```
{{< /tab >}}
{{< /tabs >}}

### Type Inference
The table ecosystem (similar to the SQL standard) is a strongly typed API. Therefore, both procedure parameters and return types must be mapped to a [data type]({{< ref "docs/dev/table/types" >}}).

From a logical perspective, the planner needs information about expected types, precision, and scale. From a JVM perspective, the planner needs information about how internal data structures are represented as JVM objects when calling a procedure.

The logic for validating input arguments and deriving data types for both the parameters and the result of a procedure is summarized under the term _type inference_.

Flink's procedures implement an automatic type inference extraction that derives data types from the procedure's class and its `call` methods via reflection. If this implicit reflective extraction approach is not successful, the extraction process can be supported by annotating affected parameters, classes, or methods with `@DataTypeHint` and `@ProcedureHint`. More examples on how to annotate procedures are shown below.

Note: although the return type in `call` method must be array type `T[]`, if use `@DataTypeHint` to annotate the return type, it's actually expected to annotate the component type of the array type, which is actually `T`.

#### Automatic Type Inference

The automatic type inference inspects the procedure's class and `call` methods to derive data types for the arguments and result of a procedure. `@DataTypeHint` and `@ProcedureHint` annotations support the automatic extraction.

For a full list of classes that can be implicitly mapped to a data type, please refer to the [data type extraction section]({{< ref "docs/dev/table/types" >}}#data-type-extraction).

**`@DataTypeHint`**

In many scenarios, it is required to support the automatic extraction _inline_ for parameters and return types of a procedure

The following example shows how to use data type hints. More information can be found in the documentation of the annotation class.

{{< tabs "9414057c-3051-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.procedure.ProcedureContext
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.types.Row;

// procedure with overloaded call methods
public static class OverloadedProcedure implements Procedure {

  // no hint required
  public Long[] call(ProcedureContext context, long a, long b) {
    return new Long[] {a + b};
  }

  // define the precision and scale of a decimal
  public @DataTypeHint("DECIMAL(12, 3)") BigDecimal[] call(ProcedureContext context, double a, double b) {
    return new BigDecimal[] {BigDecimal.valueOf(a + b)};
  }

  // define a nested data type
  @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
  public Row[] call(ProcedureContext context, int i) {
    return new Row[] {Row.of(String.valueOf(i), Instant.ofEpochSecond(i))};
  }

  // allow wildcard input and custom serialized output
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
import org.apache.flink.table.procedure.ProcedureContext
import org.apache.flink.table.procedures.Procedure
import org.apache.flink.types.Row

// procedure with overloaded call methods
class OverloadedProcedure extends Procedure {

  // no hint required
  def call(context: ProcedureContext, a: Long, b: Long): Array[Long] = {
    Array(a + b)
  }

  // define the precision and scale of a decimal
  @DataTypeHint("DECIMAL(12, 3)")
  def call(context: ProcedureContext, a: Double, b: Double): Array[BigDecimal] = {
    Array(BigDecimal.valueOf(a + b))
  }

  // define a nested data type
  @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
  def call(context: ProcedureContext, i: Integer): Array[Row] = {
    Row.of(java.lang.String.valueOf(i), java.time.Instant.ofEpochSecond(i))
  }

  // allow wildcard input and custom serialized output
  @DataTypeHint(value = "RAW", bridgedTo = classOf[java.nio.ByteBuffer])
  def call(context: ProcedureContext, @DataTypeHint(inputGroup = InputGroup.ANY) o: Object): Array[java.nio.ByteBuffer] = {
    Array[MyUtils.serializeToByteBuffer(o)]
  }
}

```
{{< /tab >}}
{{< /tabs >}}

**`@ProcedureHint`**

In some scenarios, it is desirable that one `call` method handles multiple different data types at the same time. Furthermore, in some scenarios, overloaded `call` methods have a common result type that should be declared only once.

The `@ProcedureHint` annotation can provide a mapping from argument data types to a result data type. It enables annotating entire procedure classes or `call` methods for input and result data types. One or more annotations can be declared on top of a class or individually for each `call` method for overloading procedure signatures. All hint parameters are optional. If a parameter is not defined, the default reflection-based extraction is used. Hint parameters defined on top of a procedure class are inherited by all `call` methods.

The following example shows how to use procedure hints. More information can be found in the documentation of the annotation class.

{{< tabs "16d50628-3052-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.types.Row;

// procedure with overloaded call methods
// but globally defined output type
@ProcedureHint(output = @DataTypeHint("ROW<s STRING, i INT>"))
public static class OverloadedProcedure implements Procedure {

  public Row[] call(ProcedureContext context, int a, int b) {
    return new Row[] {Row.of("Sum", a + b)};
  }

  // overloading of arguments is still possible
  public Row[] call(ProcedureContext context) {
    return new Row[] {Row.of("Empty args", -1)};
  }
}

// decouples the type inference from call methods,
// the type inference is entirely determined by the procedure hints
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

  // an implementer just needs to make sure that a method exists
  // that can be called by the JVM
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

// procedure with overloaded call methods
// but globally defined output type
@ProcedureHint(output = new DataTypeHint("ROW<s STRING, i INT>"))
class OverloadedFunction extends Procedure {

  def call(context: ProcedureContext, a: Int, b: Int): Array[Row] = {
    Array(Row.of("Sum", Int.box(a + b)))
  }

  // overloading of arguments is still possible
  def call(context: ProcedureContext): Array[Row] = {
    Array(Row.of("Empty args", Int.box(-1)))
  }
}

// decouples the type inference from call methods,
// the type inference is entirely determined by the function hints
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

  // an implementer just needs to make sure that a method exists
  // that can be called by the JVM
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


### Return Procedure in Catalog
After implementing a procedure, the catalog can then return the procedure in method `Catalog.getProcedure(ObjectPath procedurePath)`. The following example shows how to return it in a catalog.
Also, it's expected to list all the procedures in method `Catalog.listProcedures(String dbName)`.

{{< tabs "2e7d4b78-3052-11ee-be56-0242ac120002" >}}
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

// catalog with built-in procedures
public static class CatalogWithBuiltInProcedure extends GenericInMemoryCatalog {
  static {
    PROCEDURE_MAP.put(ObjectPath.fromString("system.generate_n"), new GenerateSequenceProcedure());
  }

  public CatalogWithBuiltInProcedure(String name) {
    super(name);
  }
  
  @Override
  public List<String> listProcedures(String dbName) throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(dbName)) {
      throw new DatabaseNotExistException(getName(), dbName);
    }
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

// catalog with built-in procedures
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


## Examples

The following example shows how to provide a procedure in a `Catalog` and call it with `CALL` statement. See the [Implementation Guide](#implementation-guide) for more details.

{{< tabs "1ed81b8a-3052-11ee-be56-0242ac120002" >}}
{{< tab "Java" >}}
```java

import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;

// first implement a procedure
public class GenerateSequenceProcedure implements Procedure {

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

// then provide the procedure in a custom catalog
public static class CatalogWithBuiltInProcedure extends GenericInMemoryCatalog { 

  static {
    PROCEDURE_MAP.put(ObjectPath.fromString("system.generate_n"), new GenerateSequenceProcedure());
  }
  // emit some methods
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
// register the catalog
tEnv.registerCatalog("my_catalog", new CatalogWithBuiltInProcedure());
// call the procedure with CALL statement
tEnv.executeSql("call my_catalog.`system`.generate_n(5)");

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

import org.apache.flink.table.catalog.{GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.catalog.exceptions.{CatalogException, ProcedureNotExistException}
import org.apache.flink.table.procedure.ProcedureContext
import org.apache.flink.table.procedures.Procedure

// first implement a procedure
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

// then provide the procedure in a custom catalog
class CatalogWithBuiltInProcedure(name: String) extends GenericInMemoryCatalog(name) {

  val PROCEDURE_MAP = collection.immutable.HashMap[ObjectPath, Procedure](ObjectPath.fromString("system.generate_n"),
    new GenerateSequenceProcedure());

  // emit some methods
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
// register the catalog
tEnv.registerCatalog("my_catalog", new CatalogWithBuiltInProcedure())
// call the procedure with CALL statement
tEnv.executeSql("call my_catalog.`system`.generate_n(5)")

```
{{< /tab >}}
{{< /tabs >}}



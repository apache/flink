---
title: "概览"
weight: 1
type: docs
aliases:
  - /zh/dev/types_serialization.html
  - /zh/internals/types_serialization.html
  - /zh/docs/dev/serialization/types_serialization/
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

# 数据类型以及序列化

Apache Flink 以其独特的方式来处理数据类型以及序列化，这种方式包括它自身的类型描述符、泛型类型提取以及类型序列化框架。 本文档描述了它们背后的概念和基本原理。

## Supported Data Types

Flink places some restrictions on the type of elements that can be in a DataStream.
The reason for this is that the system analyzes the types to determine
efficient execution strategies.

There are seven different categories of data types:

1. **Java Tuples** and **Scala Case Classes**
2. **Java POJOs**
3. **Primitive Types**
4. **Regular Classes**
5. **Values**
6. **Hadoop Writables**
7. **Special Types**

#### Tuples and Case Classes

{{< tabs "e24bb87b-46e8-4f17-8054-c3400aaa6ddc" >}}
{{< tab "Java" >}}

Tuples are composite types that contain a fixed number of fields with various types.
The Java API provides classes from `Tuple1` up to `Tuple25`. Every field of a tuple
can be an arbitrary Flink type including further tuples, resulting in nested tuples. Fields of a
tuple can be accessed directly using the field's name as `tuple.f4`, or using the generic getter method
`tuple.getField(int position)`. The field indices start at 0. Note that this stands in contrast
to the Scala tuples, but it is more consistent with Java's general indexing.

```java
DataStream<Tuple2<String, Integer>> wordCounts = env.fromElements(
    new Tuple2<String, Integer>("hello", 1),
    new Tuple2<String, Integer>("world", 2));

wordCounts.map(new MapFunction<Tuple2<String, Integer>, Integer>() {
    @Override
    public Integer map(Tuple2<String, Integer> value) throws Exception {
        return value.f1;
    }
});

wordCounts.keyBy(value -> value.f0);


```

{{< /tab >}}
{{< tab "Scala" >}}

Scala case classes (and Scala tuples which are a special case of case classes), are composite types that contain a fixed number of fields with various types. Tuple fields are addressed by their 1-offset names such as `_1` for the first field. Case class fields are accessed by their name.

```scala
case class WordCount(word: String, count: Int)
val input = env.fromElements(
    WordCount("hello", 1),
    WordCount("world", 2)) // Case Class Data Set

input.keyBy(_.word)

val input2 = env.fromElements(("hello", 1), ("world", 2)) // Tuple2 Data Set

input2.keyBy(value => (value._1, value._2))
```

{{< /tab >}}
{{< /tabs >}}

#### POJOs

Java and Scala classes are treated by Flink as a special POJO data type if they fulfill the following requirements:

- The class must be public.

- It must have a public constructor without arguments (default constructor).

- All fields are either public or must be accessible through getter and setter functions. For a field called `foo` the getter and setter methods must be named `getFoo()` and `setFoo()`.

- The type of a field must be supported by a registered serializer.

POJOs are generally represented with a `PojoTypeInfo` and serialized with the `PojoSerializer` (using [Kryo](https://github.com/EsotericSoftware/kryo) as configurable fallback).
The exception is when the POJOs are actually Avro types (Avro Specific Records) or produced as "Avro Reflect Types". 
In that case the POJO's are represented by an `AvroTypeInfo` and serialized with the `AvroSerializer`.
You can also register your own custom serializer if required; see [Serialization](https://nightlies.apache.org/flink/flink-docs-stable/dev/types_serialization.html#serialization-of-pojo-types) for further information.

Flink analyzes the structure of POJO types, i.e., it learns about the fields of a POJO. As a result POJO types are easier to use than general types. Moreover, Flink can process POJOs more efficiently than general types.

The following example shows a simple POJO with two public fields.

{{< tabs "0589f3b3-76d8-4913-9595-276da92cbc77" >}}
{{< tab "Java" >}}
```java
public class WordWithCount {

    public String word;
    public int count;

    public WordWithCount() {}

    public WordWithCount(String word, int count) {
        this.word = word;
        this.count = count;
    }
}

DataStream<WordWithCount> wordCounts = env.fromElements(
    new WordWithCount("hello", 1),
    new WordWithCount("world", 2));

wordCounts.keyBy(value -> value.word);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class WordWithCount(var word: String, var count: Int) {
    def this() {
      this(null, -1)
    }
}

val input = env.fromElements(
    new WordWithCount("hello", 1),
    new WordWithCount("world", 2)) // Case Class Data Set

input.keyBy(_.word)

```
{{< /tab >}}
{{< /tabs >}}

#### Primitive Types

Flink supports all Java and Scala primitive types such as `Integer`, `String`, and `Double`.

#### General Class Types

Flink supports most Java and Scala classes (API and custom).
Restrictions apply to classes containing fields that cannot be serialized, like file pointers, I/O streams, or other native
resources. Classes that follow the Java Beans conventions work well in general.

All classes that are not identified as POJO types (see POJO requirements above) are handled by Flink as general class types.
Flink treats these data types as black boxes and is not able to access their content (e.g., for efficient sorting). General types are de/serialized using the serialization framework [Kryo](https://github.com/EsotericSoftware/kryo).

#### Values

*Value* types describe their serialization and deserialization manually. Instead of going through a
general purpose serialization framework, they provide custom code for those operations by means of
implementing the `org.apache.flink.types.Value` interface with the methods `read` and `write`. Using
a Value type is reasonable when general purpose serialization would be highly inefficient. An
example would be a data type that implements a sparse vector of elements as an array. Knowing that
the array is mostly zero, one can use a special encoding for the non-zero elements, while the
general purpose serialization would simply write all array elements.

The `org.apache.flink.types.CopyableValue` interface supports manual internal cloning logic in a
similar way.

Flink comes with pre-defined Value types that correspond to basic data types. (`ByteValue`,
`ShortValue`, `IntValue`, `LongValue`, `FloatValue`, `DoubleValue`, `StringValue`, `CharValue`,
`BooleanValue`). These Value types act as mutable variants of the basic data types: Their value can
be altered, allowing programmers to reuse objects and take pressure off the garbage collector.


#### Hadoop Writables

You can use types that implement the `org.apache.hadoop.Writable` interface. The serialization logic
defined in the `write()`and `readFields()` methods will be used for serialization.

#### Special Types

You can use special types, including Scala's `Either`, `Option`, and `Try`.
The Java API has its own custom implementation of `Either`.
Similarly to Scala's `Either`, it represents a value of two possible types, *Left* or *Right*.
`Either` can be useful for error handling or operators that need to output two different types of records.

#### Type Erasure & Type Inference

*Note: This Section is only relevant for Java.*

The Java compiler throws away much of the generic type information after compilation. This is
known as *type erasure* in Java. It means that at runtime, an instance of an object does not know
its generic type any more. For example, instances of `DataStream<String>` and `DataStream<Long>` look the
same to the JVM.

Flink requires type information at the time when it prepares the program for execution (when the
main method of the program is called). The Flink Java API tries to reconstruct the type information
that was thrown away in various ways and store it explicitly in the data sets and operators. You can
retrieve the type via `DataStream.getType()`. The method returns an instance of `TypeInformation`,
which is Flink's internal way of representing types.

The type inference has its limits and needs the "cooperation" of the programmer in some cases.
Examples for that are methods that create data sets from collections, such as
`StreamExecutionEnvironment.fromCollection(),` where you can pass an argument that describes the type. But
also generic functions like `MapFunction<I, O>` may need extra type information.

The
{{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/java/typeutils/ResultTypeQueryable.java" name="ResultTypeQueryable" >}}
interface can be implemented by input formats and functions to tell the API
explicitly about their return type. The *input types* that the functions are invoked with can
usually be inferred by the result types of the previous operations.

{{< top >}}


## Type handling in Flink

Flink tries to infer a lot of information about the data types that are exchanged and stored during the distributed computation.
Think about it like a database that infers the schema of tables. In most cases, Flink infers all necessary information seamlessly
by itself. Having the type information allows Flink to do some cool things:

* The more Flink knows about data types, the better the serialization and data layout schemes are.
  That is quite important for the memory usage paradigm in Flink (work on serialized data inside/outside the heap where ever possible
  and make serialization very cheap).

* Finally, it also spares users in the majority of cases from worrying about serialization frameworks and having to register types.

In general, the information about data types is needed during the *pre-flight phase* - that is, when the program's calls on `DataStream` are made, and before any call to `execute()`, `print()`, `count()`, or `collect()`.


## Most Frequent Issues

The most frequent issues where users need to interact with Flink's data type handling are:

* **Registering subtypes:** If the function signatures describe only the supertypes, but they actually use subtypes of those during execution,
  it may increase performance a lot to make Flink aware of these subtypes.
  For that, call `.registerType(clazz)` on the `StreamExecutionEnvironment` for each subtype.

* **Registering custom serializers:** Flink falls back to [Kryo](https://github.com/EsotericSoftware/kryo) for the types that it does not handle transparently
  by itself. Not all types are seamlessly handled by Kryo (and thus by Flink). For example, many Google Guava collection types do not work well
  by default. The solution is to register additional serializers for the types that cause problems.
  Call `.getConfig().addDefaultKryoSerializer(clazz, serializer)` on the `StreamExecutionEnvironment`.
  Additional Kryo serializers are available in many libraries. See [Custom Serializers]({{< ref "docs/dev/datastream/fault-tolerance/serialization/custom_serializers" >}}) for more details on working with custom serializers.

* **Adding Type Hints:** Sometimes, when Flink cannot infer the generic types despite all tricks, a user must pass a *type hint*. That is generally
  only necessary in the Java API. The [Type Hints Section](#type-hints-in-the-java-api) describes that in more detail.

* **Manually creating a `TypeInformation`:** This may be necessary for some API calls where it is not possible for Flink to infer
  the data types due to Java's generic type erasure. See [Creating a TypeInformation or TypeSerializer](#creating-a-typeinformation-or-typeserializer)
  for details.


## Flink's TypeInformation class

The class {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/typeinfo/TypeInformation.java" name="TypeInformation" >}}
is the base class for all type descriptors. It reveals some basic properties of the type and can generate serializers
and, in specializations, comparators for the types.
(*Note that comparators in Flink do much more than defining an order - they are basically the utility to handle keys*)

Internally, Flink makes the following distinctions between types:

* Basic types: All Java primitives and their boxed form, plus `void`, `String`, `Date`, `BigDecimal`, and `BigInteger`.

* Primitive arrays and Object arrays

* Composite types

  * Flink Java Tuples (part of the Flink Java API): max 25 fields, null fields not supported

  * Scala *case classes* (including Scala tuples): null fields not supported

  * Row: tuples with arbitrary number of fields and support for null fields

  * POJOs: classes that follow a certain bean-like pattern

* Auxiliary types (Option, Either, Lists, Maps, ...)

* Generic types: These will not be serialized by Flink itself, but by Kryo.

POJOs are of particular interest, because they support the creation of complex types.
They are also transparent to the runtime and can be handled very efficiently by Flink.


#### Rules for POJO types

Flink recognizes a data type as a POJO type (and allows "by-name" field referencing) if the following
conditions are fulfilled:

* The class is public and standalone (no non-static inner class)
* The class has a public no-argument constructor
* All non-static, non-transient fields in the class (and all superclasses) are either public (and non-final)
  or have a public getter- and a setter- method that follows the Java beans
  naming conventions for getters and setters.

Note that when a user-defined data type can't be recognized as a POJO type, it must be processed as GenericType and
serialized with Kryo.


#### Creating a TypeInformation or TypeSerializer

To create a TypeInformation object for a type, use the language specific way:

{{< tabs "013807cc-9f3f-4f91-a770-26b1e5e8c85c" >}}
{{< tab "Java" >}}
Because Java generally erases generic type information, you need to pass the type to the TypeInformation
construction:

For non-generic types, you can pass the Class:
```java
TypeInformation<String> info = TypeInformation.of(String.class);
```

For generic types, you need to "capture" the generic type information via the `TypeHint`:
```java
TypeInformation<Tuple2<String, Double>> info = TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){});
```
Internally, this creates an anonymous subclass of the TypeHint that captures the generic information to preserve it
until runtime.
{{< /tab >}}
{{< tab "Scala" >}}
In Scala, Flink uses *macros* that runs at compile time and captures all generic type information while it is
still available.
```scala
// important: this import is needed to access the 'createTypeInformation' macro function
import org.apache.flink.streaming.api.scala._

val stringInfo: TypeInformation[String] = createTypeInformation[String]

val tupleInfo: TypeInformation[(String, Double)] = createTypeInformation[(String, Double)]
```

You can still use the same method as in Java as a fallback.
{{< /tab >}}
{{< /tabs >}}

To create a `TypeSerializer`, simply call `typeInfo.createSerializer(config)` on the `TypeInformation` object.

The `config` parameter is of type `ExecutionConfig` and holds the information about the program's registered
custom serializers. Where ever possibly, try to pass the programs proper ExecutionConfig. You can usually
obtain it from `DataStream` via calling `getExecutionConfig()`. Inside functions (like `MapFunction`), you can
get it by making the function a [Rich Function]() and calling `getRuntimeContext().getExecutionConfig()`.

--------
--------

## Type Information in the Scala API

Scala has very elaborate concepts for runtime type information though *type manifests* and *class tags*. In
general, types and methods have access to the types of their generic parameters - thus, Scala programs do
not suffer from type erasure as Java programs do.

In addition, Scala allows to run custom code in the Scala Compiler through Scala Macros - that means that some Flink
code gets executed whenever you compile a Scala program written against Flink's Scala API.

We use the Macros to look at the parameter types and return types of all user functions during compilation - that
is the point in time when certainly all type information is perfectly available. Within the macro, we create
a *TypeInformation* for the function's return types (or parameter types) and make it part of the operation.


#### No Implicit Value for Evidence Parameter Error

In the case where TypeInformation could not be created, programs fail to compile with an error
stating *"could not find implicit value for evidence parameter of type TypeInformation"*.

A frequent reason if that the code that generates the TypeInformation has not been imported.
Make sure to import the entire flink.api.scala package.
```scala
import org.apache.flink.api.scala._
```

Another common cause are generic methods, which can be fixed as described in the following section.


#### Generic Methods

Consider the following case below:

```scala
def selectFirst[T](input: DataStream[(T, _)]) : DataStream[T] = {
  input.map { v => v._1 }
}

val data : DataStream[(String, Long) = ...

val result = selectFirst(data)
```

For such generic methods, the data types of the function parameters and return type may not be the same
for every call and are not known at the site where the method is defined. The code above will result
in an error that not enough implicit evidence is available.

In such cases, the type information has to be generated at the invocation site and passed to the
method. Scala offers *implicit parameters* for that.

The following code tells Scala to bring a type information for *T* into the function. The type
information will then be generated at the sites where the method is invoked, rather than where the
method is defined.

```scala
def selectFirst[T : TypeInformation](input: DataStream[(T, _)]) : DataStream[T] = {
  input.map { v => v._1 }
}
```


--------
--------


## Type Information in the Java API

In the general case, Java erases generic type information. Flink tries to reconstruct as much type information
as possible via reflection, using the few bits that Java preserves (mainly function signatures and subclass information).
This logic also contains some simple type inference for cases where the return type of a function depends on its input type:

```java
public class AppendOne<T> implements MapFunction<T, Tuple2<T, Long>> {

    public Tuple2<T, Long> map(T value) {
        return new Tuple2<T, Long>(value, 1L);
    }
}
```

There are cases where Flink cannot reconstruct all generic type information. In that case, a user has to help out via *type hints*.


#### Type Hints in the Java API

In cases where Flink cannot reconstruct the erased generic type information, the Java API
offers so called *type hints*. The type hints tell the system the type of
the data stream or data set produced by a function:

```java
DataStream<SomeType> result = stream
    .map(new MyGenericNonInferrableFunction<Long, SomeType>())
        .returns(SomeType.class);
```

The `returns` statement specifies the produced type, in this case via a class. The hints support
type definition via

* Classes, for non-parameterized types (no generics)
* TypeHints in the form of `returns(new TypeHint<Tuple2<Integer, SomeType>>(){})`. The `TypeHint` class
  can capture generic type information and preserve it for the runtime (via an anonymous subclass).


#### Type extraction for Java 8 lambdas

Type extraction for Java 8 lambdas works differently than for non-lambdas, because lambdas are not associated
with an implementing class that extends the function interface.

Currently, Flink tries to figure out which method implements the lambda and uses Java's generic signatures to
determine the parameter types and the return type. However, these signatures are not generated for lambdas
by all compilers (as of writing this document only reliably by the Eclipse JDT compiler from 4.5 onwards).


#### Serialization of POJO types

The `PojoTypeInfo` is creating serializers for all the fields inside the POJO. Standard types such as
int, long, String etc. are handled by serializers we ship with Flink.
For all other types, we fall back to [Kryo](https://github.com/EsotericSoftware/kryo).

If Kryo is not able to handle the type, you can ask the `PojoTypeInfo` to serialize the POJO using [Avro](https://avro.apache.org).
To do so, you have to call

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().enableForceAvro();
```

Note that Flink is automatically serializing POJOs generated by Avro with the Avro serializer.

If you want your **entire** POJO Type to be treated by the Kryo serializer, set

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().enableForceKryo();
```

If Kryo is not able to serialize your POJO, you can add a custom serializer to Kryo, using
```java
env.getConfig().addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass)
```

There are different variants of these methods available.


## Disabling Kryo Fallback

There are cases when programs may want to explicitly avoid using Kryo as a fallback for generic types. The most
common one is wanting to ensure that all types are efficiently serialized either through Flink's own serializers,
or via user-defined custom serializers.

The setting below will raise an exception whenever a data type is encountered that would go through Kryo:
```java
env.getConfig().disableGenericTypes();
```


## Defining Type Information using a Factory

A type information factory allows for plugging-in user-defined type information into the Flink type system.
You have to implement `org.apache.flink.api.common.typeinfo.TypeInfoFactory` to return your custom type information. 
The factory is called during the type extraction phase if the corresponding type has been annotated 
with the `@org.apache.flink.api.common.typeinfo.TypeInfo` annotation. 

Type information factories can be used in both the Java and Scala API.

In a hierarchy of types the closest factory 
will be chosen while traversing upwards, however, a built-in factory has highest precedence. A factory has 
also higher precedence than Flink's built-in types, therefore you should know what you are doing.

The following example shows how to annotate a custom type `MyTuple` and supply custom type information for it using a factory in Java.

The annotated custom type:
```java
@TypeInfo(MyTupleTypeInfoFactory.class)
public class MyTuple<T0, T1> {
  public T0 myfield0;
  public T1 myfield1;
}
```

The factory supplying custom type information:
```java
public class MyTupleTypeInfoFactory extends TypeInfoFactory<MyTuple> {

  @Override
  public TypeInformation<MyTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
    return new MyTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
  }
}
```

The method `createTypeInfo(Type, Map<String, TypeInformation<?>>)` creates type information for the type the factory is targeted for. 
The parameters provide additional information about the type itself as well as the type's generic type parameters if available.

If your type contains generic parameters that might need to be derived from the input type of a Flink function, make sure to also 
implement `org.apache.flink.api.common.typeinfo.TypeInformation#getGenericParameters` for a bidirectional mapping of generic 
parameters to type information.

{{< top >}}

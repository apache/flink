---
title: "Overview"
weight: 1
type: docs
aliases:
  - /dev/types_serialization.html
  - /internals/types_serialization.html
  - /docs/dev/serialization/types_serialization/
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

# Data Types & Serialization

Apache Flink handles data types and serialization in a unique way, containing its own type descriptors,
generic type extraction, and type serialization framework. This document describes the concepts and the rationale behind them.

## Supported Data Types

Flink places some restrictions on the type of elements that can be in a DataStream.
The reason for this is that the system analyzes the types to determine
efficient execution strategies.

There are eight different categories of data types:

1. **Java Tuples**
2. **Java POJOs**
3. **Primitive Types**
4. **Common Collection Types**
5. **Regular Classes**
6. **Values**
7. **Hadoop Writables**
8. **Special Types**

#### Tuples

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

#### POJOs

Java classes are treated by Flink as a special POJO data type if they fulfill the following requirements:

- The class must be public.

- It must have a public constructor without arguments (default constructor).

- All fields are either public or must be accessible through getter and setter functions. For a field called `foo` the getter and setter methods must be named `getFoo()` and `setFoo()`.

- The type of a field must be supported by a registered serializer.

POJOs are generally represented with a `PojoTypeInfo` and serialized with the `PojoSerializer` (using [Kryo](https://github.com/EsotericSoftware/kryo) as configurable fallback).
The exception is when the POJOs are actually Avro types (Avro Specific Records) or produced as "Avro Reflect Types". 
In that case the POJO's are represented by an `AvroTypeInfo` and serialized with the `AvroSerializer`.
You can also register your own custom serializer if required; see [Serialization](https://nightlies.apache.org/flink/flink-docs-stable/dev/types_serialization.html#serialization-of-pojo-types) for further information.

Flink analyzes the structure of POJO types, i.e., it learns about the fields of a POJO. As a result POJO types are easier to use than general types. Moreover, Flink can process POJOs more efficiently than general types.

You can test whether your class adheres to the POJO requirements via `org.apache.flink.types.PojoTestUtils#assertSerializedAsPojo()` from the `flink-test-utils`.
If you additionally want to ensure that no field of the POJO will be serialized with Kryo, use `assertSerializedAsPojoWithoutKryo()` instead.

The following example shows a simple POJO with two public fields.

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

#### Primitive Types

Flink supports all Java primitive types such as `Integer`, `String`, and `Double`.

#### Common Collection Types

Flink comes with dedicated serialization support for common Java collection types, which is more efficient than going
through a general purpose serialization framework by avoiding analysis and serialization of the type metadata. 
Currently, only `Map`, `List`, `Set` and its super interface `Collection`
are supported. To utilize it, you need to declare the collection type with:

1. Concrete type arguments: e.g. `List<String>` but not `List`, `List<T>`, or `List<?>`, as Flink needs them to dispatch
   serialization of the element types.
2. Interface types: e.g. `List<String>` but not `LinkedList<String>`, as Flink does not preserve the underlying
   implementation types across serialization.

Other nonqualified collection types will be handled by Flink as general class types. If the implementation types are
also required to be preserved, you also need to register it with a custom serializer.

#### General Class Types

Flink supports most Java classes (API and custom).
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
  For that, register serializers for each subtype via
  [pipeline.serialization-config]({{< ref "docs/deployment/config#pipeline-serialization-config" >}}):

```yaml
pipeline.serialization-config:
# register serializer for POJO types
  - org.example.MyCustomType1: {type: pojo, class: org.example.MyCustomSerializer1}
# register serializer for generic types with Kryo
  - org.example.MyCustomType2: {type: kryo, kryo-type: registered, class: org.example.MyCustomSerializer2}
```

You could also programmatically set it as follows (note that the more compact flow-style YAML is used in the code example,
while block-style YAML list is used in the above example for better readability):

```java
Configuration config = new Configuration();
config.set(PipelineOptions.SERIALIZATION_CONFIG,
    List.of("org.example.MyCustomType1: {type: pojo, class: org.example.MyCustomSerializer1}",
        "org.example.MyCustomType2: {type: kryo, kryo-type: registered, class: org.example.MyCustomSerializer2}"));
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
```

* **Registering custom serializers:** Flink falls back to [Kryo](https://github.com/EsotericSoftware/kryo) for the types that it does not handle transparently
  by itself. Not all types are seamlessly handled by Kryo (and thus by Flink). For example, many Google Guava collection types do not work well
  by default. The solution is to register additional serializers for the types that cause problems via
  [pipeline.serialization-config]({{< ref "docs/deployment/config#pipeline-serialization-config" >}}):

```yaml
pipeline.serialization-config:
  - org.example.MyCustomType: {type: kryo, kryo-type: default, class: org.example.MyCustomSerializer}
```
  
  Additional Kryo serializers are available in many libraries. See [3rd party serializer]({{< ref "docs/dev/datastream/fault-tolerance/serialization/third_party_serializers" >}}) for more details on working with external serializers.

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

Because Java generally erases generic type information, you need to pass the type to the TypeInformation
construction.

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

There are two ways to create a TypeSerializer.

The first is to simply call `typeInfo.createSerializer(config)` on the `TypeInformation` object.
The `config` parameter is of type `SerializerConfig` and holds the information about the program's registered
custom serializers. Where ever possibly, try to pass the programs proper `SerializerConfig`.

The second is to use `getRuntimeContext().createSerializer(typeInfo)` within a function. Inside functions
(like `MapFunction`), you can get it by making the function a
[Rich Function]({{< ref "docs/dev/datastream/user_defined_functions#rich-functions" >}}) and calling
`getRuntimeContext().createSerializer(typeInfo)`.


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
determine the parameter types and the return type. However, these signatures are not generated for lambdas by all compilers.
If you observe unexpected behavior, manually specify the return type using the `returns` method.


#### Serialization of POJO types

The `PojoTypeInfo` is creating serializers for all the fields inside the POJO. Standard types such as
int, long, String etc. are handled by serializers we ship with Flink.
For all other types, we fall back to [Kryo](https://github.com/EsotericSoftware/kryo).

If Kryo is not able to handle the type, you can ask the `PojoTypeInfo` to serialize the POJO using [Avro](https://avro.apache.org).
To do so, make sure to include the `flink-avro` module, and set:

```yaml
pipeline.force-avro: true 
```

Note that Flink is automatically serializing POJOs generated by Avro with the Avro serializer.

If you want your **entire** POJO Type to be treated by the Kryo serializer, set:

```yaml
pipeline.force-kryo: true 
```

If Kryo is not able to serialize your POJO, you can add a custom serializer to Kryo,
using [pipeline.serialization-config]({{< ref "docs/deployment/config#pipeline-serialization-config" >}}):

```yaml
pipeline.serialization-config:
  - org.example.MyCustomType: {type: kryo, kryo-type: registered, class: org.example.MyCustomSerializer}
```

## Disabling Kryo Fallback

There are cases when programs may want to explicitly avoid using Kryo as a fallback for generic types. The most
common one is wanting to ensure that all types are efficiently serialized either through Flink's own serializers,
or via user-defined custom serializers. To do that, set:

```yaml
pipeline.generic-types: false 
```

An exception will be raised whenever a data type is encountered that would go through Kryo.

## Defining Type Information using a Factory

A type information factory allows for plugging-in user-defined type information into the Flink type system.
You have to implement `org.apache.flink.api.common.typeinfo.TypeInfoFactory` to return your custom type information. 
The factory is called during the type extraction phase to supply custom type information for the corresponding type.

In a hierarchy of types, the closest factory will be chosen while traversing upwards.
However, a built-in factory has the highest precedence. A factory also has higher precedence than
Flink's built-in types, therefore you should know what you are doing.

You can associate your custom type information factory with the corresponding type via the configuration option
[pipeline.serialization-config]({{< ref "docs/deployment/config#pipeline-serialization-config" >}}):

```yaml
pipeline.serialization-config:
  - org.example.MyCustomType: {type: typeinfo, class: org.example.MyCustomTypeInfoFactory}
``` 

Alternatively, you can annotate either the corresponding type or a POJO's field using this type with the 
`@org.apache.flink.api.common.typeinfo.TypeInfo` annotation to have the factory associated.
Note that the type information factory associated via configuration option will have higher precedence. 

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

Instead of annotating the type itself, which may not be possible for third-party code, you can also
annotate the usage of this type inside a valid Flink POJO like this:
```java
public class MyPojo {
  public int id;

  @TypeInfo(MyTupleTypeInfoFactory.class)
  public MyTuple<Integer, String> tuple;
}
```

The method `createTypeInfo(Type, Map<String, TypeInformation<?>>)` creates type information for the type the factory is targeted for. 
The parameters provide additional information about the type itself as well as the type's generic type parameters if available.

If your type contains generic parameters that might need to be derived from the input type of a Flink function, make sure to also 
implement `org.apache.flink.api.common.typeinfo.TypeInformation#getGenericParameters` for a bidirectional mapping of generic 
parameters to type information.

{{< top >}}

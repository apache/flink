---
title: "Data Types & Serialization"
nav-id: types
nav-parent_id: dev
nav-show_overview: true
nav-pos: 50
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

Apache Flink handles data types and serialization in a unique way, containing its own type descriptors,
generic type extraction, and type serialization framework. This document describes the concepts and the rationale behind them.

* This will be replaced by the TOC
{:toc}


## Type handling in Flink

Flink tries to infer a lot of information about the data types that are exchanged and stored during the distributed computation.
Think about it like a database that infers the schema of tables. In most cases, Flink infers all necessary information seamlessly
by itself. Having the type information allows Flink to do some cool things:

* Using POJOs types and grouping / joining / aggregating them by referring to field names (like `dataSet.keyBy("username")`).
  The type information allows Flink to check (for typos and type compatibility) early rather than failing later at runtime.

* The more Flink knows about data types, the better the serialization and data layout schemes are.
  That is quite important for the memory usage paradigm in Flink (work on serialized data inside/outside the heap where ever possible
  and make serialization very cheap).

* Finally, it also spares users in the majority of cases from worrying about serialization frameworks and having to register types.

In general, the information about data types is needed during the *pre-flight phase* - that is, when the program's calls on `DataStream`
and `DataSet` are made, and before any call to `execute()`, `print()`, `count()`, or `collect()`.


## Most Frequent Issues

The most frequent issues where users need to interact with Flink's data type handling are:

* **Registering subtypes:** If the function signatures describe only the supertypes, but they actually use subtypes of those during execution,
  it may increase performance a lot to make Flink aware of these subtypes.
  For that, call `.registerType(clazz)` on the `StreamExecutionEnvironment` or `ExecutionEnvironment` for each subtype.

* **Registering custom serializers:** Flink falls back to [Kryo](https://github.com/EsotericSoftware/kryo) for the types that it does not handle transparently
  by itself. Not all types are seamlessly handled by Kryo (and thus by Flink). For example, many Google Guava collection types do not work well
  by default. The solution is to register additional serializers for the types that cause problems.
  Call `.getConfig().addDefaultKryoSerializer(clazz, serializer)` on the `StreamExecutionEnvironment` or `ExecutionEnvironment`.
  Additional Kryo serializers are available in many libraries. See [Custom Serializers]({{ site.baseurl }}/dev/custom_serializers.html) for more details on working with custom serializers.

* **Adding Type Hints:** Sometimes, when Flink cannot infer the generic types despite all tricks, a user must pass a *type hint*. That is generally
  only necessary in the Java API. The [Type Hints Section](#type-hints-in-the-java-api) describes that in more detail.

* **Manually creating a `TypeInformation`:** This may be necessary for some API calls where it is not possible for Flink to infer
  the data types due to Java's generic type erasure. See [Creating a TypeInformation or TypeSerializer](#creating-a-typeinformation-or-typeserializer)
  for details.


## Flink's TypeInformation class

The class {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/typeinfo/TypeInformation.java "TypeInformation" %}
is the base class for all type descriptors. It reveals some basic properties of the type and can generate serializers
and, in specializations, comparators for the types.
(*Note that comparators in Flink do much more than defining an order - they are basically the utility to handle keys*)

Internally, Flink makes the following distinctions between types:

* Basic types: All Java primitives and their boxed form, plus `void`, `String`, `Date`, `BigDecimal`, and `BigInteger`.

* Primitive arrays and Object arrays

* Composite types

  * Flink Java Tuples (part of the Flink Java API): max 25 fields, null fields not supported

  * Scala *case classes* (including Scala tuples): max 22 fields, null fields not supported

  * Row: tuples with arbitrary number of fields and support for null fields

  * POJOs: classes that follow a certain bean-like pattern

* Auxiliary types (Option, Either, Lists, Maps, ...)

* Generic types: These will not be serialized by Flink itself, but by Kryo.

POJOs are of particular interest, because they support the creation of complex types and the use of field
names in the definition of keys: `dataSet.join(another).where("name").equalTo("personName")`.
They are also transparent to the runtime and can be handled very efficiently by Flink.


#### Rules for POJO types

Flink recognizes a data type as a POJO type (and allows "by-name" field referencing) if the following
conditions are fulfilled:

* The class is public and standalone (no non-static inner class)
* The class has a public no-argument constructor
* All non-static, non-transient fields in the class (and all superclasses) are either public (and non-final)
  or have a public getter- and a setter- method that follows the Java beans
  naming conventions for getters and setters.


#### Creating a TypeInformation or TypeSerializer

To create a TypeInformation object for a type, use the language specific way:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
Because Java generally erases generic type information, you need to pass the type to the TypeInformation
construction:

For non-generic types, you can pass the Class:
{% highlight java %}
TypeInformation<String> info = TypeInformation.of(String.class);
{% endhighlight %}

For generic types, you need to "capture" the generic type information via the `TypeHint`:
{% highlight java %}
TypeInformation<Tuple2<String, Double>> info = TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){});
{% endhighlight %}
Internally, this creates an anonymous subclass of the TypeHint that captures the generic information to preserve it
until runtime.
</div>

<div data-lang="scala" markdown="1">
In Scala, Flink uses *macros* that runs at compile time and captures all generic type information while it is
still available.
{% highlight scala %}
// important: this import is needed to access the 'createTypeInformation' macro function
import org.apache.flink.streaming.api.scala._

val stringInfo: TypeInformation[String] = createTypeInformation[String]

val tupleInfo: TypeInformation[(String, Double)] = createTypeInformation[(String, Double)]
{% endhighlight %}

You can still use the same method as in Java as a fallback.
</div>
</div>

To create a `TypeSerializer`, simply call `typeInfo.createSerializer(config)` on the `TypeInformation` object.

The `config` parameter is of type `ExecutionConfig` and holds the information about the program's registered
custom serializers. Where ever possibly, try to pass the programs proper ExecutionConfig. You can usually
obtain it from `DataStream` or `DataSet` via calling `getExecutionConfig()`. Inside functions (like `MapFunction`), you can
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
{% highlight scala %}
import org.apache.flink.api.scala._
{% endhighlight %}

Another common cause are generic methods, which can be fixed as described in the following section.


#### Generic Methods

Consider the following case below:

{% highlight scala %}
def selectFirst[T](input: DataSet[(T, _)]) : DataSet[T] = {
  input.map { v => v._1 }
}

val data : DataSet[(String, Long) = ...

val result = selectFirst(data)
{% endhighlight %}

For such generic methods, the data types of the function parameters and return type may not be the same
for every call and are not known at the site where the method is defined. The code above will result
in an error that not enough implicit evidence is available.

In such cases, the type information has to be generated at the invocation site and passed to the
method. Scala offers *implicit parameters* for that.

The following code tells Scala to bring a type information for *T* into the function. The type
information will then be generated at the sites where the method is invoked, rather than where the
method is defined.

{% highlight scala %}
def selectFirst[T : TypeInformation](input: DataSet[(T, _)]) : DataSet[T] = {
  input.map { v => v._1 }
}
{% endhighlight %}


--------
--------


## Type Information in the Java API

In the general case, Java erases generic type information. Flink tries to reconstruct as much type information
as possible via reflection, using the few bits that Java preserves (mainly function signatures and subclass information).
This logic also contains some simple type inference for cases where the return type of a function depends on its input type:

{% highlight java %}
public class AppendOne<T> extends MapFunction<T, Tuple2<T, Long>> {

    public Tuple2<T, Long> map(T value) {
        return new Tuple2<T, Long>(value, 1L);
    }
}
{% endhighlight %}

There are cases where Flink cannot reconstruct all generic type information. In that case, a user has to help out via *type hints*.


#### Type Hints in the Java API

In cases where Flink cannot reconstruct the erased generic type information, the Java API
offers so called *type hints*. The type hints tell the system the type of
the data stream or data set produced by a function:

{% highlight java %}
DataSet<SomeType> result = dataSet
    .map(new MyGenericNonInferrableFunction<Long, SomeType>())
        .returns(SomeType.class);
{% endhighlight %}

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

The PojoTypeInformation is creating serializers for all the fields inside the POJO. Standard types such as
int, long, String etc. are handled by serializers we ship with Flink.
For all other types, we fall back to Kryo.

If Kryo is not able to handle the type, you can ask the PojoTypeInfo to serialize the POJO using Avro.
To do so, you have to call

{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().enableForceAvro();
{% endhighlight %}

Note that Flink is automatically serializing POJOs generated by Avro with the Avro serializer.

If you want your **entire** POJO Type to be treated by the Kryo serializer, set

{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().enableForceKryo();
{% endhighlight %}

If Kryo is not able to serialize your POJO, you can add a custom serializer to Kryo, using
{% highlight java %}
env.getConfig().addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass)
{% endhighlight %}

There are different variants of these methods available.


## Disabling Kryo Fallback

There are cases when programs may want to explicitly avoid using Kryo as a fallback for generic types. The most
common one is wanting to ensure that all types are efficiently serialized either through Flink's own serializers,
or via user-defined custom serializers.

The setting below will raise an exception whenever a data type is encountered that would go through Kryo:
{% highlight java %}
env.getConfig().disableGenericTypes();
{% endhighlight %}


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
{% highlight java %}
@TypeInfo(MyTupleTypeInfoFactory.class)
public class MyTuple<T0, T1> {
  public T0 myfield0;
  public T1 myfield1;
}
{% endhighlight %}

The factory supplying custom type information:
{% highlight java %}
public class MyTupleTypeInfoFactory extends TypeInfoFactory<MyTuple> {

  @Override
  public TypeInformation<MyTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
    return new MyTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
  }
}
{% endhighlight %}

The method `createTypeInfo(Type, Map<String, TypeInformation<?>>)` creates type information for the type the factory is targeted for. 
The parameters provide additional information about the type itself as well as the type's generic type parameters if available.

If your type contains generic parameters that might need to be derived from the input type of a Flink function, make sure to also 
implement `org.apache.flink.api.common.typeinfo.TypeInformation#getGenericParameters` for a bidirectional mapping of generic 
parameters to type information.

{% top %}

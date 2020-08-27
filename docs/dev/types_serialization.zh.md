---
title: "数据类型以及序列化"
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

Apache Flink 以其独特的方式来处理数据类型以及序列化，这种方式包括它自身的类型描述符、泛型类型提取以及类型序列化框架。
本文档描述了它们背后的概念和基本原理。

* This will be replaced by the TOC
{:toc}

## Supported Data Types

Flink places some restrictions on the type of elements that can be in a DataSet or DataStream.
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

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Tuples are composite types that contain a fixed number of fields with various types.
The Java API provides classes from `Tuple1` up to `Tuple25`. Every field of a tuple
can be an arbitrary Flink type including further tuples, resulting in nested tuples. Fields of a
tuple can be accessed directly using the field's name as `tuple.f4`, or using the generic getter method
`tuple.getField(int position)`. The field indices start at 0. Note that this stands in contrast
to the Scala tuples, but it is more consistent with Java's general indexing.

{% highlight java %}
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


{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

Scala case classes (and Scala tuples which are a special case of case classes), are composite types that contain a fixed number of fields with various types. Tuple fields are addressed by their 1-offset names such as `_1` for the first field. Case class fields are accessed by their name.

{% highlight scala %}
case class WordCount(word: String, count: Int)
val input = env.fromElements(
    WordCount("hello", 1),
    WordCount("world", 2)) // Case Class Data Set

input.keyBy(_.word)

val input2 = env.fromElements(("hello", 1), ("world", 2)) // Tuple2 Data Set

input2.keyBy(value => (value._1, value._2))
{% endhighlight %}

</div>
</div>

#### POJOs

Java and Scala classes are treated by Flink as a special POJO data type if they fulfill the following requirements:

- The class must be public.

- It must have a public constructor without arguments (default constructor).

- All fields are either public or must be accessible through getter and setter functions. For a field called `foo` the getter and setter methods must be named `getFoo()` and `setFoo()`.

- The type of a field must be supported by a registered serializer.

POJOs are generally represented with a `PojoTypeInfo` and serialized with the `PojoSerializer` (using [Kryo](https://github.com/EsotericSoftware/kryo) as configurable fallback).
The exception is when the POJOs are actually Avro types (Avro Specific Records) or produced as "Avro Reflect Types".
In that case the POJO's are represented by an `AvroTypeInfo` and serialized with the `AvroSerializer`.
You can also register your own custom serializer if required; see [Serialization](https://ci.apache.org/projects/flink/flink-docs-stable/dev/types_serialization.html#serialization-of-pojo-types) for further information.

Flink analyzes the structure of POJO types, i.e., it learns about the fields of a POJO. As a result POJO types are easier to use than general types. Moreover, Flink can process POJOs more efficiently than general types.

The following example shows a simple POJO with two public fields.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class WordWithCount(var word: String, var count: Int) {
    def this() {
      this(null, -1)
    }
}

val input = env.fromElements(
    new WordWithCount("hello", 1),
    new WordWithCount("world", 2)) // Case Class Data Set

input.keyBy(_.word)

{% endhighlight %}
</div>
</div>

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
`ExecutionEnvironment.fromCollection(),` where you can pass an argument that describes the type. But
also generic functions like `MapFunction<I, O>` may need extra type information.

The
{% gh_link /flink-core/src/main/java/org/apache/flink/api/java/typeutils/ResultTypeQueryable.java "ResultTypeQueryable" %}
interface can be implemented by input formats and functions to tell the API
explicitly about their return type. The *input types* that the functions are invoked with can
usually be inferred by the result types of the previous operations.

{% top %}

## Flink 中的类型处理

Flink 会尽力推断有关数据类型的大量信息，这些数据会在分布式计算期间被网络交换或存储。
可以把它想象成一个推断表结构的数据库。在大多数情况下，Flink 可以依赖自身透明的推断出所有需要的类型信息。
掌握这些类型信息可以帮助 Flink 实现很多意想不到的特性：

* Flink 对数据类型了解的越多，序列化和数据布局方案就越好。
  这对 Flink 中的内存使用范式尤为重要（可以尽可能处理堆上或者堆外的序列化数据并且使序列化操作很廉价）。

* 最后，它还使用户在大多数情况下免于担心序列化框架以及类型注册。

通常在应用*运行之前的阶段 (pre-flight phase)*，需要数据的类型信息 - 也就是在程序对 `DataStream` 或者
`DataSet` 的操作调用之后，在 `execute()`、`print()`、`count()`、`collect()` 调用之前。


## 最常见问题

用户需要与 Flink 数据类型处理进行交互的最常见问题是：

* **注册子类型** 如果函数签名只包含超类型，但它们实际上在执行期间使用那些类型的子类型，则使 Flink 感知这些子类型可能会大大提高性能。
  可以为每一个子类型调用 `StreamExecutionEnvironment` 或者 `ExecutionEnvironment` 的 `.registerType(clazz)` 方法。

* **注册自定义序列化器：** 当 Flink 无法通过自身处理类型时会回退到 [Kryo](https://github.com/EsotericSoftware/kryo) 进行处理。
  并非所有的类型都可以被 Kryo (或者 Flink ) 处理。例如谷歌的 Guava 集合类型默认情况下是没办法很好处理的。
  解决方案是为这些引起问题的类型注册额外的序列化器。调用 `StreamExecutionEnvironment` 或者 `ExecutionEnvironment` 
  的 `.getConfig().addDefaultKryoSerializer(clazz, serializer)` 方法注册 Kryo 序列化器。存在很多的额外 Kryo 序列化器类库
  具体细节可以参看 [自定义序列化器]({{ site.baseurl }}/zh/dev/custom_serializers.html) 以了解更多的自定义序列化器。

* **添加类型提示** 有时， Flink 用尽一切手段也无法推断出泛型类型，用户需要提供*类型提示*。通常只在 Java API 中需要。
  [类型提示部分](#java-api-中的类型提示) 描述了更多的细节。

* **手动创建 `TypeInformation`：** 这可能是某些 API 调用所必需的，因为 Java 的泛型类型擦除会导致 Flink 无法推断出数据类型。
  参考 [创建 TypeInformation 或者 TypeSerializer](#创建-typeinformation-或者-typeserializer)

## Flink 的 TypeInformation 类

类 {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/typeinfo/TypeInformation.java "TypeInformation" %}
是所有类型描述符的基类。该类表示类型的基本属性，并且可以生成序列化器，在一些特殊情况下可以生成类型的比较器。
(*请注意，Flink 中的比较器不仅仅是定义顺序 - 它们是处理键的基础工具*)

Flink 内部对类型做了如下区分：

* 基础类型：所有的 Java 主类型（primitive）以及他们的包装类，再加上 `void`、`String`、`Date`、`BigDecimal` 以及 `BigInteger`。

* 主类型数组（primitive array）以及对象数组

* 复合类型

  * Flink 中的 Java 元组 (Tuples) (元组是 Flink Java API 的一部分)：最多支持25个字段，null 是不支持的。 

  * Scala 中的 *case classes* (包括 Scala 元组)：null 是不支持的。

  * Row：具有任意数量字段的元组并且支持 null 字段。。

  * POJOs: 遵循某种类似 bean 模式的类。

* 辅助类型 (Option、Either、Lists、Maps 等)

* 泛型类型：这些不是由 Flink 本身序列化的，而是由 Kryo 序列化的。

POJOs 是特别有趣的，因为他们支持复杂类型的创建以及在键的定义中直接使用字段名：
`dataSet.join(another).where("name").equalTo("personName")`
它们对运行时也是透明的，并且可以由 Flink 非常高效地处理。

#### POJO 类型的规则

如果满足以下条件，Flink 会将数据类型识别为 POJO 类型（并允许“按名称”引用字段）：

* 该类是公有的 (public) 和独立的（没有非静态内部类）
* 该类拥有公有的无参构造器
* 类（以及所有超类）中的所有非静态、非 transient 字段都是公有的（非 final 的），
  或者具有遵循 Java bean 对于 getter 和 setter 命名规则的公有 getter 和 setter 方法。

请注意，当用户自定义的数据类型无法识别为 POJO 类型时，必须将其作为泛型类型处理并使用 Kryo 进行序列化。

#### 创建 TypeInformation 或者 TypeSerializer

要为类型创建 TypeInformation 对象，需要使用特定于语言的方法：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
因为 Java 会擦除泛型类型信息，所以需要将类型传入 TypeInformation 构造函数：

对于非泛型类型，可以传入类型的 Class 对象：
{% highlight java %}
TypeInformation<String> info = TypeInformation.of(String.class);
{% endhighlight %}

对于泛型类型，你需要通过 `TypeHint` 来“捕获”泛型类型信息：
{% highlight java %}
TypeInformation<Tuple2<String, Double>> info = TypeInformation.of(new TypeHint<Tuple2<String, Double>>(){});
{% endhighlight %}
在内部，这会创建 TypeHint 的匿名子类，捕获泛型信息并会将其保留到运行时。
</div>

<div data-lang="scala" markdown="1">
在 Scala 中，Flink 使用在编译时运行的*宏*捕获所有泛型类型信息。
{% highlight scala %}
// 注意：这个导入是为了访问 'createTypeInformation' 的宏函数
import org.apache.flink.streaming.api.scala._

val stringInfo: TypeInformation[String] = createTypeInformation[String]

val tupleInfo: TypeInformation[(String, Double)] = createTypeInformation[(String, Double)]
{% endhighlight %}

仍然可以使用与 Java 相同的方法作为后备。
</div>
</div>

通过调用 `TypeInformation` 对象的 `typeInfo.createSerializer(config)` 方法可以简单的创建 `TypeSerializer` 对象。

`config` 参数的类型是 `ExecutionConfig`，这个参数中持有程序注册的自定义序列化器信息。
尽可能传入程序合适的 ExecutionConfig 。可以通过调用 `DataStream` 或者 `DataSet` 的 `getExecutionConfig()`
函数获得 ExecutionConfig 对象。如果是在一个函数的内部 （比如 `MapFunction`），可以使这个函数首先成为
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/functions/RichFunction.java "RichFunction" %}
，然后通过调用 `getRuntimeContext().getExecutionConfig()` 获得 ExecutionConfig 对象。

--------
--------

## Scala API 中的类型信息

Scala 拥有精细的运行时类型信息概念 *type manifests* 以及 *class tags*。类型和方法通常可以访问他们泛型
参数的类型 - 因此，Scala 程序不像 Java 程序那样需要面对类型擦除的问题。

此外，Scala 允许通过 Scala 宏在 Scala 编译器中运行自定义代码 - 这意味着无论何时编译使用 Flink Scala API 
编写的 Scala 程序，都会执行一些 Flink 代码。

我们在编译期间使用宏来查看所有用户自定义函数的参数类型和返回类型 - 这是所有类型信息都完全可用的时间点。
在宏中，我们为函数的返回类型（或参数类型）创建了 *TypeInformation*，并使其成为操作的一部分。


#### No Implicit Value for Evidence Parameter 错误

当 TypeInformation 创建失败的时候，程序会抛出错误信息 *"could not find implicit value for evidence parameter of type TypeInformation"*

产生这样错误最常见的原因是没有导入生成 TypeInformation 的代码。需要导入完整的 flink.api.scala  包。
{% highlight scala %}
import org.apache.flink.api.scala._
{% endhighlight %}

另一个常见原因是泛型方法，可以按照以下部分所述进行修复。


#### 泛型方法

考虑下面的代码：

{% highlight scala %}
def selectFirst[T](input: DataSet[(T, _)]) : DataSet[T] = {
  input.map { v => v._1 }
}

val data : DataSet[(String, Long) = ...

val result = selectFirst(data)
{% endhighlight %}

对于上面这样的泛型方法，函数的参数类型以及返回类型可能每一次调用都是不同的并且没有办法在方法定义的时候被感知到。
上面的代码将导致没有足够的隐式转换可用错误。

在这种情况下，类型信息必须在调用时间点生成并将其传递给方法。Scala 为此提供了*隐式转换*。

下面的代码告诉 Scala 将 *T* 的类型信息带入函数中。类型信息会在方法被调用的时间生成，而不是
方法定义的时候。

{% highlight scala %}
def selectFirst[T : TypeInformation](input: DataSet[(T, _)]) : DataSet[T] = {
  input.map { v => v._1 }
}
{% endhighlight %}


--------
--------


## Java API 中的类型信息

Java 会擦除泛型类型信息。Flink 使用 Java 预留的少量位（主要是函数签名以及子类信息）通过反射尽可能多的重新构造类型信息。
对于依赖输入类型来确定函数返回类型的情况，此逻辑还包含一些简单类型推断：

{% highlight java %}
public class AppendOne<T> implements MapFunction<T, Tuple2<T, Long>> {

    public Tuple2<T, Long> map(T value) {
        return new Tuple2<T, Long>(value, 1L);
    }
}
{% endhighlight %}

在某些情况下，Flink 无法重建所有泛型类型信息。 在这种情况下，用户必须通过*类型提示*来解决问题。

#### Java API 中的类型提示

在 Flink 无法重建被擦除的泛型类型信息的情况下，Java API 需要提供所谓的*类型提示*。
类型提示告诉系统 DateStream 或者 DateSet 产生的类型：

{% highlight java %}
DataSet<SomeType> result = dataSet
    .map(new MyGenericNonInferrableFunction<Long, SomeType>())
        .returns(SomeType.class);
{% endhighlight %}

在上面情况下 `returns` 表达通过 Class 类型指出产生的类型。通过下面方式支持类型提示：

* 对于非参数化的类型（没有泛型）的 Class 类型
* 以 `returns(new TypeHint<Tuple2<Integer, SomeType>>(){})` 方式进行类型提示。
  `TypeHint` 类可以捕获泛型的类型信息并且保存到执行期间（通过匿名子类）。

#### Java 8 lambdas 的类型提取

Java 8 lambdas 的类型提取与非-lambdas 不同，因为 lambdas 与扩展函数接口的实现类没有关联。

Flink 目前试图找出实现 lambda 的方法，并使用 Java 的泛型签名来确定参数类型和返回类型。 
但是，并非所有编译器都为 lambda 生成这些签名（此文档写作时，只有 Eclipse JDT 编译器从4.5开始可靠支持）。


#### POJO 类型的序列化

PojoTypeInfo 为 POJO 中的所有字段创建序列化器。Flink 标准类型如 int、long、String 等由 Flink 序列化器处理。
对于所有其他类型，我们回退到 Kryo。

对于 Kryo 不能处理的类型，你可以要求 PojoTypeInfo 使用 Avro 对 POJO 进行序列化。
需要通过下面的代码开启。

{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().enableForceAvro();
{% endhighlight %}

请注意，Flink 会使用 Avro 序列化器自动序列化 Avro 生成的 POJO。

通过下面设置可以让你的**整个** POJO 类型被 Kryo 序列化器处理。

{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().enableForceKryo();
{% endhighlight %}

如果 Kryo 不能序列化你的 POJO，可以通过下面的代码添加自定义的序列化器
{% highlight java %}
env.getConfig().addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass)
{% endhighlight %}

这些方法有不同的变体可供选择。


## 禁止回退到 Kryo

对于泛型信息，程序可能希望在一些情况下显示的避免使用 Kryo。最常见的场景是，用户想要确保所有的类型都可以通过 Flink 自身
或者用户自定义的序列化器高效的进行序列化操作。

下面的设置将引起通过 Kryo 的数据类型抛出异常：
{% highlight java %}
env.getConfig().disableGenericTypes();
{% endhighlight %}


## 使用工厂方法定义类型信息

类型信息工厂允许将用户定义的类型信息插入 Flink 类型系统。
你可以通过实现 `org.apache.flink.api.common.typeinfo.TypeInfoFactory` 来返回自定义的类型信息工厂。
如果相应的类型已指定了 `@org.apache.flink.api.common.typeinfo.TypeInfo` 注解，则在类型提取阶段会调用 TypeInfo 注解指定的
类型信息工厂。

类型信息工厂可以在 Java 和 Scala API 中使用。

在类型的层次结构中，在向上遍历时将选择最近的工厂，但是内置工厂具有最高优先级。
工厂的优先级也高于 Flink 的内置类型，因此你应该知道自己在做什么。

以下示例说明如何使用 Java 中的工厂注释为自定义类型 `MyTuple` 提供自定义类型信息。

带注解的自定义类型：
{% highlight java %}
@TypeInfo(MyTupleTypeInfoFactory.class)
public class MyTuple<T0, T1> {
  public T0 myfield0;
  public T1 myfield1;
}
{% endhighlight %}

支持自定义类型信息的工厂：
{% highlight java %}
public class MyTupleTypeInfoFactory extends TypeInfoFactory<MyTuple> {

  @Override
  public TypeInformation<MyTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
    return new MyTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
  }
}
{% endhighlight %}

方法 `createTypeInfo(Type, Map<String, TypeInformation<?>>)` 为工厂所对应的类型创建类型信息。
参数提供有关类型本身的额外信息以及类型的泛型类型参数。

如果你的类型包含可能需要从 Flink 函数的输入类型派生的泛型参数，
请确保还实现了 `org.apache.flink.api.common.typeinfo.TypeInformation#getGenericParameters`，
以便将泛型参数与类型信息进行双向映射。

{% top %}

---
title: "Java Lambda Expressions"
weight: 300
type: docs
bookToc: false
aliases:
  - /dev/java_lambdas.html
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

# Java Lambda Expressions

Java 8 introduced several new language features designed for faster and clearer coding. With the most important feature,
the so-called "Lambda Expressions", it opened the door to functional programming. Lambda expressions allow for implementing and
passing functions in a straightforward way without having to declare additional (anonymous) classes.

{{< hint info >}}
Flink supports the usage of lambda expressions for all operators of the Java API, however, whenever a lambda expression uses Java generics you need to declare type information *explicitly*. 
{{< /hint >}}

This document shows how to use lambda expressions and describes current
limitations. For a general introduction to the Flink API, please refer to the
[DataSteam API overview]({{< ref "docs/dev/datastream/overview" >}})

### Examples and Limitations

The following example illustrates how to implement a simple, inline `map()` function that squares its input using a lambda expression.
The types of input `i` and output parameters of the `map()` function need not to be declared as they are inferred by the Java compiler.

```java
env.fromElements(1, 2, 3)
// returns the squared i
.map(i -> i*i)
.print();
```

Flink can automatically extract the result type information from the implementation of the method signature `OUT map(IN value)` because `OUT` is not generic but `Integer`.

Unfortunately, functions such as `flatMap()` with a signature `void flatMap(IN value, Collector<OUT> out)` are compiled into `void flatMap(IN value, Collector out)` by the Java compiler. This makes it impossible for Flink to infer the type information for the output type automatically.

Flink will most likely throw an exception similar to the following:

```
org.apache.flink.api.common.functions.InvalidTypesException: The generic type parameters of 'Collector' are missing.
    In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved.
    An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface.
    Otherwise the type has to be specified explicitly using type information.
```

In this case, the type information needs to be *specified explicitly*, otherwise the output will be treated as type `Object` which leads to unefficient serialization.

```java
DataStream<Integer> input = env.fromElements(1, 2, 3);

// collector type must be declared
input.flatMap((Integer number, Collector<String> out) -> {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < number; i++) {
        builder.append("a");
        out.collect(builder.toString());
    }
})
// provide type information explicitly
.returns(Types.STRING)
// prints "a", "a", "aa", "a", "aa", "aaa"
.print();
```

Similar problems occur when using a `map()` function with a generic return type. A method signature `Tuple2<Integer, Integer> map(Integer value)` is erasured to `Tuple2 map(Integer value)` in the example below.

```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

env.fromElements(1, 2, 3)
    .map(i -> Tuple2.of(i, i))    // no information about fields of Tuple2
    .print();
```

In general, those problems can be solved in multiple ways:

```java
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

// use the explicit ".returns(...)"
env.fromElements(1, 2, 3)
    .map(i -> Tuple2.of(i, i))
    .returns(Types.TUPLE(Types.INT, Types.INT))
    .print();

// use a class instead
env.fromElements(1, 2, 3)
    .map(new MyTuple2Mapper())
    .print();

public static class MyTuple2Mapper extends MapFunction<Integer, Tuple2<Integer, Integer>> {
    @Override
    public Tuple2<Integer, Integer> map(Integer i) {
        return Tuple2.of(i, i);
    }
}

// use an anonymous class instead
env.fromElements(1, 2, 3)
    .map(new MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer i) {
            return Tuple2.of(i, i);
        }
    })
    .print();

// or in this example use a tuple subclass instead
env.fromElements(1, 2, 3)
    .map(i -> new DoubleTuple(i, i))
    .print();

public static class DoubleTuple extends Tuple2<Integer, Integer> {
    public DoubleTuple(int f0, int f1) {
        this.f0 = f0;
        this.f1 = f1;
    }
}
```

{{< top >}}

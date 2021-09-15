---
title: 事件处理 (CEP)
weight: 2
type: docs
aliases:
  - /zh/dev/libs/cep.html
  - /zh/apis/streaming/libs/cep.html
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

# FlinkCEP - Flink的复杂事件处理

FlinkCEP是在Flink上层实现的复杂事件处理库。
它可以让你在无限事件流中检测出特定的事件模型，有机会掌握数据中重要的那部分。

本页讲述了Flink CEP中可用的API，我们首先讲述[模式API](#模式api)，它可以让你指定想在数据流中检测的模式，然后讲述如何[检测匹配的事件序列并进行处理](#检测模式)。
再然后我们讲述Flink在按照事件时间[处理迟到事件](#按照事件时间处理迟到事件)时的假设，
以及如何从旧版本的Flink向1.3之后的版本[迁移作业](#从旧版本迁移13之前)。



## 开始

如果你想现在开始尝试，[创建一个Flink程序]({{< ref "docs/dev/datastream/project-configuration" >}})，
添加FlinkCEP的依赖到项目的`pom.xml`文件中。

{{< tabs "722d55a5-7f12-4bcc-b080-b28d5e8860ac" >}}
{{< tab "Java" >}}
{{< artifact flink-cep withScalaVersion >}}
{{< /tab >}}
{{< tab "Scala" >}}
{{< artifact flink-cep-scala withScalaVersion >}}
{{< /tab >}}
{{< /tabs >}}

{% info 提示 %} FlinkCEP不是二进制发布包的一部分。在集群上执行如何链接它可以看[这里]({{< ref "docs/dev/datastream/project-configuration" >}})。

现在可以开始使用Pattern API写你的第一个CEP程序了。

{% warn 注意 %} `DataStream`中的事件，如果你想在上面进行模式匹配的话，必须实现合适的 `equals()`和`hashCode()`方法，
因为FlinkCEP使用它们来比较和匹配事件。

{{< tabs "4fef83d9-e4c5-4073-9607-4c8cde1ebf1e" >}}
{{< tab "Java" >}}
```java
DataStream<Event> input = ...

Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
        new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) {
                return event.getId() == 42;
            }
        }
    ).next("middle").subtype(SubEvent.class).where(
        new SimpleCondition<SubEvent>() {
            @Override
            public boolean filter(SubEvent subEvent) {
                return subEvent.getVolume() >= 10.0;
            }
        }
    ).followedBy("end").where(
         new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) {
                return event.getName().equals("end");
            }
         }
    );

PatternStream<Event> patternStream = CEP.pattern(input, pattern);

DataStream<Alert> result = patternStream.process(
    new PatternProcessFunction<Event, Alert>() {
        @Override
        public void processMatch(
                Map<String, List<Event>> pattern,
                Context ctx,
                Collector<Alert> out) throws Exception {
            out.collect(createAlertFrom(pattern));
        }
    });
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[Event] = ...

val pattern = Pattern.begin[Event]("start").where(_.getId == 42)
  .next("middle").subtype(classOf[SubEvent]).where(_.getVolume >= 10.0)
  .followedBy("end").where(_.getName == "end")

val patternStream = CEP.pattern(input, pattern)

val result: DataStream[Alert] = patternStream.process(
    new PatternProcessFunction[Event, Alert]() {
        override def processMatch(
              `match`: util.Map[String, util.List[Event]],
              ctx: PatternProcessFunction.Context,
              out: Collector[Alert]): Unit = {
            out.collect(createAlertFrom(pattern))
        }
    })
```
{{< /tab >}}
{{< /tabs >}}

## 模式API

模式API可以让你定义想从输入流中抽取的复杂模式序列。

每个复杂的模式序列包括多个简单的模式，比如，寻找拥有相同属性事件序列的模式。从现在开始，我们把这些简单的模式称作**模式**，
把我们在数据流中最终寻找的复杂模式序列称作**模式序列**，你可以把模式序列看作是这样的模式构成的图，
这些模式基于用户指定的**条件**从一个转换到另外一个，比如 `event.getName().equals("end")`。
一个**匹配**是输入事件的一个序列，这些事件通过一系列有效的模式转换，能够访问到复杂模式图中的所有模式。

{% warn 注意 %} 每个模式必须有一个独一无二的名字，你可以在后面使用它来识别匹配到的事件。

{% warn 注意 %} 模式的名字不能包含字符`":"`.

这一节的剩余部分我们会先讲述如何定义[单个模式](#单个模式)，然后讲如何将单个模式组合成[复杂模式](#组合模式)。

### 单个模式

一个**模式**可以是一个**单例**或者**循环**模式。单例模式只接受一个事件，循环模式可以接受多个事件。
在模式匹配表达式中，模式`"a b+ c? d"`（或者`"a"`，后面跟着一个或者多个`"b"`，再往后可选择的跟着一个`"c"`，最后跟着一个`"d"`），
`a`，`c?`，和 `d`都是单例模式，`b+`是一个循环模式。默认情况下，模式都是单例的，你可以通过使用[量词](#量词)把它们转换成循环模式。
每个模式可以有一个或者多个[条件](#条件)来决定它接受哪些事件。

#### 量词

在FlinkCEP中，你可以通过这些方法指定循环模式：`pattern.oneOrMore()`，指定期望一个给定事件出现一次或者多次的模式（例如前面提到的`b+`模式）；
`pattern.times(#ofTimes)`，指定期望一个给定事件出现特定次数的模式，例如出现4次`a`；
`pattern.times(#fromTimes, #toTimes)`，指定期望一个给定事件出现次数在一个最小值和最大值中间的模式，比如出现2-4次`a`。

你可以使用`pattern.greedy()`方法让循环模式变成贪心的，但现在还不能让模式组贪心。
你可以使用`pattern.optional()`方法让所有的模式变成可选的，不管是否是循环模式。

对一个命名为`start`的模式，以下量词是有效的：

{{< tabs "fae84f75-d2eb-4233-b5c9-7588235ed937" >}}
{{< tab "Java" >}}
```java
// 期望出现4次
start.times(4);

// 期望出现0或者4次
start.times(4).optional();

// 期望出现2、3或者4次
start.times(2, 4);

// 期望出现2、3或者4次，并且尽可能的重复次数多
start.times(2, 4).greedy();

// 期望出现0、2、3或者4次
start.times(2, 4).optional();

// 期望出现0、2、3或者4次，并且尽可能的重复次数多
start.times(2, 4).optional().greedy();

// 期望出现1到多次
start.oneOrMore();

// 期望出现1到多次，并且尽可能的重复次数多
start.oneOrMore().greedy();

// 期望出现0到多次
start.oneOrMore().optional();

// 期望出现0到多次，并且尽可能的重复次数多
start.oneOrMore().optional().greedy();

// 期望出现2到多次
start.timesOrMore(2);

// 期望出现2到多次，并且尽可能的重复次数多
start.timesOrMore(2).greedy();

// 期望出现0、2或多次
start.timesOrMore(2).optional();

// 期望出现0、2或多次，并且尽可能的重复次数多
start.timesOrMore(2).optional().greedy();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 期望出现4次
start.times(4)

// 期望出现0或者4次
start.times(4).optional()

// 期望出现2、3或者4次
start.times(2, 4)

// 期望出现2、3或者4次，并且尽可能的重复次数多
start.times(2, 4).greedy()

// 期望出现0、2、3或者4次
start.times(2, 4).optional()

// 期望出现0、2、3或者4次，并且尽可能的重复次数多
start.times(2, 4).optional().greedy()

// 期望出现1到多次
start.oneOrMore()

// 期望出现1到多次，并且尽可能的重复次数多
start.oneOrMore().greedy()

// 期望出现0到多次
start.oneOrMore().optional()

// 期望出现0到多次，并且尽可能的重复次数多
start.oneOrMore().optional().greedy()

// 期望出现2到多次
start.timesOrMore(2)

// 期望出现2到多次，并且尽可能的重复次数多
start.timesOrMore(2).greedy()

// 期望出现0、2或多次
start.timesOrMore(2).optional()

// 期望出现0、2或多次，并且尽可能的重复次数多
start.timesOrMore(2).optional().greedy()
```
{{< /tab >}}
{{< /tabs >}}

#### 条件

对每个模式你可以指定一个条件来决定一个进来的事件是否被接受进入这个模式，例如，它的value字段应该大于5，或者大于前面接受的事件的平均值。
指定判断事件属性的条件可以通过`pattern.where()`、`pattern.or()`或者`pattern.until()`方法。
这些可以是`IterativeCondition`或者`SimpleCondition`。

**迭代条件:** 这是最普遍的条件类型。使用它可以指定一个基于前面已经被接受的事件的属性或者它们的一个子集的统计数据来决定是否接受时间序列的条件。

下面是一个迭代条件的代码，它接受"middle"模式下一个事件的名称开头是"foo"， 并且前面已经匹配到的事件加上这个事件的价格小于5.0。
迭代条件非常强大，尤其是跟循环模式结合使用时。

{{< tabs "1be4490c-24d3-4827-8427-7652e23adb63" >}}
{{< tab "Java" >}}
```java
middle.oneOrMore()
    .subtype(SubEvent.class)
    .where(new IterativeCondition<SubEvent>() {
        @Override
        public boolean filter(SubEvent value, Context<SubEvent> ctx) throws Exception {
            if (!value.getName().startsWith("foo")) {
                return false;
            }

            double sum = value.getPrice();
            for (Event event : ctx.getEventsForPattern("middle")) {
                sum += event.getPrice();
            }
            return Double.compare(sum, 5.0) < 0;
        }
    });
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
middle.oneOrMore()
    .subtype(classOf[SubEvent])
    .where(
        (value, ctx) => {
            lazy val sum = ctx.getEventsForPattern("middle").map(_.getPrice).sum
            value.getName.startsWith("foo") && sum + value.getPrice < 5.0
        }
    )
```
{{< /tab >}}
{{< /tabs >}}

{% warn 注意 %} 调用`ctx.getEventsForPattern(...)`可以获得所有前面已经接受作为可能匹配的事件。
调用这个操作的代价可能很小也可能很大，所以在实现你的条件时，尽量少使用它。

描述的上下文提供了获取事件时间属性的方法。更多细节可以看[时间上下文](#时间上下文)。

**简单条件：** 这种类型的条件扩展了前面提到的`IterativeCondition`类，它决定是否接受一个事件只取决于事件自身的属性。

{{< tabs "20073e8b-3772-4faf-894c-e1bf2cbff15e" >}}
{{< tab "Java" >}}
```java
start.where(new SimpleCondition<Event>() {
    @Override
    public boolean filter(Event value) {
        return value.getName().startsWith("foo");
    }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
start.where(event => event.getName.startsWith("foo"))
```
{{< /tab >}}
{{< /tabs >}}

最后，你可以通过`pattern.subtype(subClass)`方法限制接受的事件类型是初始事件的子类型。

{{< tabs "5011129d-6c43-4fb7-84ae-3000d2296f28" >}}
{{< tab "Java" >}}
```java
start.subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
    @Override
    public boolean filter(SubEvent value) {
        return ... // 一些判断条件
    }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
start.subtype(classOf[SubEvent]).where(subEvent => ... /* 一些判断条件 */)
```
{{< /tab >}}
{{< /tabs >}}

**组合条件：** 如上所示，你可以把`subtype`条件和其他的条件结合起来使用。这适用于任何条件，你可以通过依次调用`where()`来组合条件。
最终的结果是每个单一条件的结果的逻辑**AND**。如果想使用**OR**来组合条件，你可以像下面这样使用`or()`方法。

{{< tabs "ba6bc50b-c9f6-4534-aff8-b2957ada791b" >}}
{{< tab "Java" >}}
```java
pattern.where(new SimpleCondition<Event>() {
    @Override
    public boolean filter(Event value) {
        return ... // 一些判断条件
    }
}).or(new SimpleCondition<Event>() {
    @Override
    public boolean filter(Event value) {
        return ... // 一些判断条件
    }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
pattern.where(event => ... /* 一些判断条件 */).or(event => ... /* 一些判断条件 */)
```
{{< /tab >}}
{{< /tabs >}}


**停止条件：** 如果使用循环模式（`oneOrMore()`和`oneOrMore().optional()`），你可以指定一个停止条件，例如，接受事件的值大于5直到值的和小于50。

为了更好的理解它，看下面的例子。给定

* 模式如`"(a+ until b)"` (一个或者更多的`"a"`直到`"b"`)

* 到来的事件序列`"a1" "c" "a2" "b" "a3"`

* 输出结果会是： `{a1 a2} {a1} {a2} {a3}`.

你可以看到`{a1 a2 a3}`和`{a2 a3}`由于停止条件没有被输出。

{{< tabs "3b2dea6b-1615-47cb-bec5-2a281666dc4c" >}}
{{< tab "Java" >}}
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">模式操作</th>
            <th class="text-center">描述</th>
        </tr>
    </thead>
    <tbody>
       <tr>
            <td><strong>where(condition)</strong></td>
            <td>
                <p>为当前模式定义一个条件。为了匹配这个模式，一个事件必须满足某些条件。
                 多个连续的where()语句取与组成判断条件：</p>
```java
pattern.where(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // 一些判断条件
    }
});
```
            </td>
        </tr>
        <tr>
            <td><strong>or(condition)</strong></td>
            <td>
                <p>增加一个新的判断，和当前的判断取或。一个事件只要满足至少一个判断条件就匹配到模式：</p>
```java
pattern.where(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // 一些判断条件
    }
}).or(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // 替代条件
    }
});
```
                    </td>
       </tr>
              <tr>
                 <td><strong>until(condition)</strong></td>
                 <td>
                     <p>为循环模式指定一个停止条件。意思是满足了给定的条件的事件出现后，就不会再有事件被接受进入模式了。</p>
                     <p>只适用于和<code>oneOrMore()</code>同时使用。</p>
                     <p><b>NOTE:</b> 在基于事件的条件中，它可用于清理对应模式的状态。</p>
```java
pattern.oneOrMore().until(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // 替代条件
    }
});
```
                 </td>
              </tr>
       <tr>
           <td><strong>subtype(subClass)</strong></td>
           <td>
               <p>为当前模式定义一个子类型条件。一个事件只有是这个子类型的时候才能匹配到模式：</p>
```java
pattern.subtype(SubEvent.class);
```
           </td>
       </tr>
       <tr>
          <td><strong>oneOrMore()</strong></td>
          <td>
              <p>指定模式期望匹配到的事件至少出现一次。.</p>
              <p>默认（在子事件间）使用松散的内部连续性。
              关于内部连续性的更多信息可以参考<a href="#consecutive_java">连续性</a>。</p>
              <p><b>NOTE:</b> 推荐使用<code>until()</code>或者<code>within()</code>来清理状态。</p>
```java
pattern.oneOrMore();
```
          </td>
       </tr>
           <tr>
              <td><strong>timesOrMore(#times)</strong></td>
              <td>
                  <p>指定模式期望匹配到的事件至少出现<strong>#times</strong>次。.</p>
                  <p>默认（在子事件间）使用松散的内部连续性。
                  关于内部连续性的更多信息可以参考<a href="#consecutive_java">连续性</a>。</p>
```java
pattern.timesOrMore(2);
```
           </td>
       </tr>
       <tr>
          <td><strong>times(#ofTimes)</strong></td>
          <td>
              <p>指定模式期望匹配到的事件正好出现的次数。</p>
              <p>默认（在子事件间）使用松散的内部连续性。
              关于内部连续性的更多信息可以参考<a href="#consecutive_java">连续性</a>。</p>
```java
pattern.times(2);
```
          </td>
       </tr>
       <tr>
          <td><strong>times(#fromTimes, #toTimes)</strong></td>
          <td>
              <p>指定模式期望匹配到的事件出现次数在<strong>#fromTimes</strong>和<strong>#toTimes</strong>之间。</p>
              <p>默认（在子事件间）使用松散的内部连续性。
              关于内部连续性的更多信息可以参考<a href="#consecutive_java">连续性</a>。</p>
```java
pattern.times(2, 4);
```
          </td>
       </tr>
       <tr>
          <td><strong>optional()</strong></td>
          <td>
              <p>指定这个模式是可选的，也就是说，它可能根本不出现。这对所有之前提到的量词都适用。</p>
```java
pattern.oneOrMore().optional();
```
          </td>
       </tr>
       <tr>
          <td><strong>greedy()</strong></td>
          <td>
              <p>指定这个模式是贪心的，也就是说，它会重复尽可能多的次数。这只对量词适用，现在还不支持模式组。</p>
```java
pattern.oneOrMore().greedy();
```
          </td>
       </tr>
  </tbody>
</table>
{{< /tab >}}
{{< tab "Scala" >}}
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">模式操作</th>
            <th class="text-center">描述</th>
        </tr>
	    </thead>
    <tbody>

        <tr>
            <td><strong>where(condition)</strong></td>
            <td>
              <p>为当前模式定义一个条件。为了匹配这个模式，一个事件必须满足某些条件。
              多个连续的where()语句取与组成判断条件：</p>
```scala
pattern.where(event => ... /* 一些判断条件 */)
```
            </td>
        </tr>
        <tr>
            <td><strong>or(condition)</strong></td>
            <td>
                <p>增加一个新的判断，和当前的判断取或。一个事件只要满足至少一个判断条件就匹配到模式：</p>
```scala
pattern.where(event => ... /* 一些判断条件 */)
    .or(event => ... /* 替代条件 */)
```
                    </td>
                </tr>
<tr>
          <td><strong>until(condition)</strong></td>
          <td>
              <p>为循环模式指定一个停止条件。意思是满足了给定的条件的事件出现后，就不会再有事件被接受进入模式了。</p>
              <p>只适用于和<code>oneOrMore()</code>同时使用。</p>
              <p><b>提示：</b> 在基于事件的条件中，它可用于清理对应模式的状态。</p>
```scala
pattern.oneOrMore().until(event => ... /* 替代条件 */)
```
          </td>
       </tr>
       <tr>
           <td><strong>subtype(subClass)</strong></td>
           <td>
               <p>为当前模式定义一个子类型条件。一个事件只有是这个子类型的时候才能匹配到模式：</p>
```scala
pattern.subtype(classOf[SubEvent])
```
           </td>
       </tr>
       <tr>
          <td><strong>oneOrMore()</strong></td>
          <td>
               <p>指定模式期望匹配到的事件至少出现一次。.</p>
               <p>默认（在子事件间）使用松散的内部连续性。
               关于内部连续性的更多信息可以参考<a href="#consecutive_java">连续性</a>。</p>
               <p><b>提示：</b> 推荐使用<code>until()</code>或者<code>within()</code>来清理状态。</p>
```scala
pattern.oneOrMore()
```
          </td>
       </tr>
       <tr>
          <td><strong>timesOrMore(#times)</strong></td>
          <td>
              <p>指定模式期望匹配到的事件至少出现<strong>#times</strong>次。.</p>
              <p>默认（在子事件间）使用松散的内部连续性。
              关于内部连续性的更多信息可以参考<a href="#consecutive_java">连续性</a>。</p>
```scala
pattern.timesOrMore(2)
```
           </td>
       </tr>
       <tr>
          <td><strong>times(#ofTimes)</strong></td>
          <td>
              <p>指定模式期望匹配到的事件正好出现的次数。</p>
              <p>默认（在子事件间）使用松散的内部连续性。
              关于内部连续性的更多信息可以参考<a href="#consecutive_java">连续性</a>。</p>
```scala
pattern.times(2)
```
                 </td>
       </tr>
       <tr>
         <td><strong>times(#fromTimes, #toTimes)</strong></td>
         <td>
             <p>指定模式期望匹配到的事件出现次数在<strong>#fromTimes</strong>和<strong>#toTimes</strong>之间。</p>
             <p>默认（在子事件间）使用松散的内部连续性。
             关于内部连续性的更多信息可以参考<a href="#consecutive_java">连续性</a>。</p>
```scala
pattern.times(2, 4)
```
         </td>
       </tr>
       <tr>
          <td><strong>optional()</strong></td>
          <td>
             <p>指定这个模式是可选的，也就是说，它可能根本不出现。这对所有之前提到的量词都适用。</p>
```scala
pattern.oneOrMore().optional()
```
          </td>
       </tr>
       <tr>
          <td><strong>greedy()</strong></td>
          <td>
             <p>指定这个模式是贪心的，也就是说，它会重复尽可能多的次数。这只对量词适用，现在还不支持模式组。</p>
```scala
pattern.oneOrMore().greedy()
```
          </td>
       </tr>
  </tbody>
</table>
{{< /tab >}}
{{< /tabs >}}

### 组合模式

现在你已经看到单个的模式是什么样的了，该去看看如何把它们连接起来组成一个完整的模式序列。

模式序列由一个初始模式作为开头，如下所示：

{{< tabs "ca5e1d90-40d2-4e6b-8ffb-11850f5665b1" >}}
{{< tab "Java" >}}
```java
Pattern<Event, ?> start = Pattern.<Event>begin("start");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val start : Pattern[Event, _] = Pattern.begin("start")
```
{{< /tab >}}
{{< /tabs >}}

接下来，你可以增加更多的模式到模式序列中并指定它们之间所需的*连续条件*。FlinkCEP支持事件之间如下形式的连续策略：

 1. **严格连续**: 期望所有匹配的事件严格的一个接一个出现，中间没有任何不匹配的事件。

 2. **松散连续**: 忽略匹配的事件之间的不匹配的事件。

 3. **不确定的松散连续**: 更进一步的松散连续，允许忽略掉一些匹配事件的附加匹配。

可以使用下面的方法来指定模式之间的连续策略：

1. `next()`，指定*严格连续*，
2. `followedBy()`，指定*松散连续*，
3. `followedByAny()`，指定*不确定的松散*连续。

或者

1. `notNext()`，如果不想后面直接连着一个特定事件
2. `notFollowedBy()`，如果不想一个特定事件发生在两个事件之间的任何地方。

{% warn 注意 %} 模式序列不能以`notFollowedBy()`结尾。

{% warn 注意 %} 一个`NOT`模式前面不能是可选的模式。

{{< tabs "c5c3a1fe-8ab4-45ae-8a97-c2c2e96b6bb3" >}}
{{< tab "Java" >}}
```java

// 严格连续
Pattern<Event, ?> strict = start.next("middle").where(...);

// 松散连续
Pattern<Event, ?> relaxed = start.followedBy("middle").where(...);

// 不确定的松散连续
Pattern<Event, ?> nonDetermin = start.followedByAny("middle").where(...);

// 严格连续的NOT模式
Pattern<Event, ?> strictNot = start.notNext("not").where(...);

// 松散连续的NOT模式
Pattern<Event, ?> relaxedNot = start.notFollowedBy("not").where(...);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

// 严格连续
val strict: Pattern[Event, _] = start.next("middle").where(...)

// 松散连续
val relaxed: Pattern[Event, _] = start.followedBy("middle").where(...)

// 不确定的松散连续
val nonDetermin: Pattern[Event, _] = start.followedByAny("middle").where(...)

// 严格连续的NOT模式
val strictNot: Pattern[Event, _] = start.notNext("not").where(...)

// 松散连续的NOT模式
val relaxedNot: Pattern[Event, _] = start.notFollowedBy("not").where(...)

```
{{< /tab >}}
{{< /tabs >}}

松散连续意味着跟着的事件中，只有第一个可匹配的事件会被匹配上，而不确定的松散连接情况下，有着同样起始的多个匹配会被输出。
举例来说，模式`"a b"`，给定事件序列`"a"，"c"，"b1"，"b2"`，会产生如下的结果：

1. `"a"`和`"b"`之间严格连续： `{}` （没有匹配），`"a"`之后的`"c"`导致`"a"`被丢弃。

2. `"a"`和`"b"`之间松散连续： `{a b1}`，松散连续会"跳过不匹配的事件直到匹配上的事件"。

3. `"a"`和`"b"`之间不确定的松散连续： `{a b1}`, `{a b2}`，这是最常见的情况。

也可以为模式定义一个有效时间约束。
例如，你可以通过`pattern.within()`方法指定一个模式应该在10秒内发生。
这种时间模式支持[处理时间和事件时间]({{< ref "docs/concepts/time" >}}).

{% warn 注意 %} 一个模式序列只能有一个时间限制。如果限制了多个时间在不同的单个模式上，会使用最小的那个时间限制。

{{< tabs "8228f5b0-b6b3-4ca6-a56b-e5a8fd5fdc3b" >}}
{{< tab "Java" >}}
```java
next.within(Time.seconds(10));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
next.within(Time.seconds(10))
```
{{< /tab >}}
{{< /tabs >}}

#### 循环模式中的连续性

你可以在循环模式中使用和前面[章节](#组合模式)讲过的同样的连续性。
连续性会被运用在被接受进入模式的事件之间。
用这个例子来说明上面所说的连续性，一个模式序列`"a b+ c"`（`"a"`后面跟着一个或者多个（不确定连续的）`"b"`，然后跟着一个`"c"`）
输入为`"a"，"b1"，"d1"，"b2"，"d2"，"b3"，"c"`，输出结果如下：

 1. **严格连续**: `{a b3 c}` -- `"b1"`之后的`"d1"`导致`"b1"`被丢弃，同样`"b2"`因为`"d2"`被丢弃。

 2. **松散连续**: `{a b1 c}`，`{a b1 b2 c}`，`{a b1 b2 b3 c}`，`{a b2 c}`，`{a b2 b3 c}`，`{a b3 c}` - `"d"`都被忽略了。

 3. **不确定松散连续**: `{a b1 c}`，`{a b1 b2 c}`，`{a b1 b3 c}`，`{a b1 b2 b3 c}`，`{a b2 c}`，`{a b2 b3 c}`，`{a b3 c}` -
    注意`{a b1 b3 c}`，这是因为`"b"`之间是不确定松散连续产生的。

对于循环模式（例如`oneOrMore()`和`times()`)），默认是*松散连续*。如果想使用*严格连续*，你需要使用`consecutive()`方法明确指定，
如果想使用*不确定松散连续*，你可以使用`allowCombinations()`方法。

{{< tabs "4279c356-9c89-42e8-807b-0cee30165fc3" >}}
{{< tab "Java" >}}
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">模式操作</th>
            <th class="text-center">描述</th>
        </tr>
    </thead>
    <tbody>
       <tr>
          <td><strong>consecutive()</strong><a name="consecutive_java"></a></td>
          <td>
              <p>与<code>oneOrMore()</code>和<code>times()</code>一起使用， 在匹配的事件之间施加严格的连续性，
              也就是说，任何不匹配的事件都会终止匹配（和<code>next()</code>一样）。</p>
              <p>如果不使用它，那么就是松散连续（和<code>followedBy()</code>一样）。</p>

              <p>例如，一个如下的模式：</p>
```java
Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
  @Override
  public boolean filter(Event value) throws Exception {
    return value.getName().equals("c");
  }
})
.followedBy("middle").where(new SimpleCondition<Event>() {
  @Override
  public boolean filter(Event value) throws Exception {
    return value.getName().equals("a");
  }
}).oneOrMore().consecutive()
.followedBy("end1").where(new SimpleCondition<Event>() {
  @Override
  public boolean filter(Event value) throws Exception {
    return value.getName().equals("b");
  }
});
```
              <p>输入：C D A1 A2 A3 D A4 B，会产生下面的输出：</p>

              <p>如果施加严格连续性： {C A1 B}，{C A1 A2 B}，{C A1 A2 A3 B}</p>
              <p>不施加严格连续性： {C A1 B}，{C A1 A2 B}，{C A1 A2 A3 B}，{C A1 A2 A3 A4 B}</p>
          </td>
       </tr>
       <tr>
       <td><strong>allowCombinations()</strong><a name="allow_comb_java"></a></td>
       <td>
              <p>与<code>oneOrMore()</code>和<code>times()</code>一起使用，
              在匹配的事件中间施加不确定松散连续性（和<code>followedByAny()</code>一样）。</p>
              <p>如果不使用，就是松散连续（和<code>followedBy()</code>一样）。</p>

              <p>例如，一个如下的模式：</p>
```java
Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
  @Override
  public boolean filter(Event value) throws Exception {
    return value.getName().equals("c");
  }
})
.followedBy("middle").where(new SimpleCondition<Event>() {
  @Override
  public boolean filter(Event value) throws Exception {
    return value.getName().equals("a");
  }
}).oneOrMore().allowCombinations()
.followedBy("end1").where(new SimpleCondition<Event>() {
  @Override
  public boolean filter(Event value) throws Exception {
    return value.getName().equals("b");
  }
});
```
               <p>输入：C D A1 A2 A3 D A4 B，会产生如下的输出：</p>

               <p>如果使用不确定松散连续： {C A1 B}，{C A1 A2 B}，{C A1 A3 B}，{C A1 A4 B}，{C A1 A2 A3 B}，{C A1 A2 A4 B}，{C A1 A3 A4 B}，{C A1 A2 A3 A4 B}</p>
               <p>如果不使用：{C A1 B}，{C A1 A2 B}，{C A1 A2 A3 B}，{C A1 A2 A3 A4 B}</p>
       </td>
       </tr>
  </tbody>
</table>
{{< /tab >}}
{{< tab "Scala" >}}
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">模式操作</th>
            <th class="text-center">描述</th>
        </tr>
    </thead>
    <tbody>
           <tr>
              <td><strong>consecutive()</strong><a name="consecutive_scala"></a></td>
              <td>
                <p>与<code>oneOrMore()</code>和<code>times()</code>一起使用， 在匹配的事件之间施加严格的连续性，
                也就是说，任何不匹配的事件都会终止匹配（和<code>next()</code>一样）。</p>
                <p>如果不使用它，那么就是松散连续（和<code>followedBy()</code>一样）。</p>

          <p>例如，一个如下的模式：</p>
```scala
Pattern.begin("start").where(_.getName().equals("c"))
  .followedBy("middle").where(_.getName().equals("a"))
                       .oneOrMore().consecutive()
  .followedBy("end1").where(_.getName().equals("b"))
```

                <p>输入：C D A1 A2 A3 D A4 B，会产生下面的输出：</p>

                <p>如果施加严格连续性： {C A1 B}，{C A1 A2 B}，{C A1 A2 A3 B}</p>
                <p>不施加严格连续性： {C A1 B}，{C A1 A2 B}，{C A1 A2 A3 B}，{C A1 A2 A3 A4 B}</p>
              </td>
           </tr>
           <tr>
                  <td><strong>allowCombinations()</strong><a name="allow_comb_java"></a></td>
                  <td>
                    <p>与<code>oneOrMore()</code>和<code>times()</code>一起使用，
                    在匹配的事件中间施加不确定松散连续性（和<code>followedByAny()</code>一样）。</p>
                    <p>如果不使用，就是松散连续（和<code>followedBy()</code>一样）。</p>

           <p>例如，一个如下的模式：</p>
```scala
Pattern.begin("start").where(_.getName().equals("c"))
  .followedBy("middle").where(_.getName().equals("a"))
                       .oneOrMore().allowCombinations()
  .followedBy("end1").where(_.getName().equals("b"))
```

                          <p>输入：C D A1 A2 A3 D A4 B，会产生如下的输出：</p>

                          <p>如果使用不确定松散连续： {C A1 B}，{C A1 A2 B}，{C A1 A3 B}，{C A1 A4 B}，{C A1 A2 A3 B}，{C A1 A2 A4 B}，{C A1 A3 A4 B}，{C A1 A2 A3 A4 B}</p>
                          <p>如果不使用：{C A1 B}，{C A1 A2 B}，{C A1 A2 A3 B}，{C A1 A2 A3 A4 B}</p>
                  </td>
                  </tr>
  </tbody>
</table>
{{< /tab >}}
{{< /tabs >}}

### 模式组

也可以定义一个模式序列作为`begin`，`followedBy`，`followedByAny`和`next`的条件。这个模式序列在逻辑上会被当作匹配的条件，
并且返回一个`GroupPattern`，可以在`GroupPattern`上使用`oneOrMore()`，`times(#ofTimes)`，
`times(#fromTimes, #toTimes)`，`optional()`，`consecutive()`，`allowCombinations()`。

{{< tabs "46fe5d3e-bd35-4416-a811-d2bce9631a90" >}}
{{< tab "Java" >}}
```java

Pattern<Event, ?> start = Pattern.begin(
    Pattern.<Event>begin("start").where(...).followedBy("start_middle").where(...)
);

// 严格连续
Pattern<Event, ?> strict = start.next(
    Pattern.<Event>begin("next_start").where(...).followedBy("next_middle").where(...)
).times(3);

// 松散连续
Pattern<Event, ?> relaxed = start.followedBy(
    Pattern.<Event>begin("followedby_start").where(...).followedBy("followedby_middle").where(...)
).oneOrMore();

// 不确定松散连续
Pattern<Event, ?> nonDetermin = start.followedByAny(
    Pattern.<Event>begin("followedbyany_start").where(...).followedBy("followedbyany_middle").where(...)
).optional();

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

val start: Pattern[Event, _] = Pattern.begin(
    Pattern.begin[Event]("start").where(...).followedBy("start_middle").where(...)
)

// 严格连续
val strict: Pattern[Event, _] = start.next(
    Pattern.begin[Event]("next_start").where(...).followedBy("next_middle").where(...)
).times(3)

// 松散连续
val relaxed: Pattern[Event, _] = start.followedBy(
    Pattern.begin[Event]("followedby_start").where(...).followedBy("followedby_middle").where(...)
).oneOrMore()

// 不确定松散连续
val nonDetermin: Pattern[Event, _] = start.followedByAny(
    Pattern.begin[Event]("followedbyany_start").where(...).followedBy("followedbyany_middle").where(...)
).optional()

```
{{< /tab >}}
{{< /tabs >}}

<br />

{{< tabs "2675eeaa-8c9a-490b-a73b-55b051e796f5" >}}
{{< tab "Java" >}}
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">模式操作</th>
            <th class="text-center">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><strong>begin(#name)</strong></td>
            <td>
            <p>定义一个开始的模式：</p>
```java
Pattern<Event, ?> start = Pattern.<Event>begin("start");
```
            </td>
        </tr>
        <tr>
            <td><strong>begin(#pattern_sequence)</strong></td>
            <td>
            <p>定义一个开始的模式：</p>
```java
Pattern<Event, ?> start = Pattern.<Event>begin(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);
```
            </td>
        </tr>
        <tr>
            <td><strong>next(#name)</strong></td>
            <td>
                <p>增加一个新的模式。匹配的事件必须是直接跟在前面匹配到的事件后面（严格连续）：</p>
```java
Pattern<Event, ?> next = start.next("middle");
```
            </td>
        </tr>
        <tr>
            <td><strong>next(#pattern_sequence)</strong></td>
            <td>
                <p>增加一个新的模式。匹配的事件序列必须是直接跟在前面匹配到的事件后面（严格连续）：</p>
```java
Pattern<Event, ?> next = start.next(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);
```
            </td>
        </tr>
        <tr>
            <td><strong>followedBy(#name)</strong></td>
            <td>
                <p>增加一个新的模式。可以有其他事件出现在匹配的事件和之前匹配到的事件中间（松散连续）：</p>
```java
Pattern<Event, ?> followedBy = start.followedBy("middle");
```
            </td>
        </tr>
        <tr>
            <td><strong>followedBy(#pattern_sequence)</strong></td>
            <td>
                 <p>增加一个新的模式。可以有其他事件出现在匹配的事件序列和之前匹配到的事件中间（松散连续）：</p>
```java
Pattern<Event, ?> followedBy = start.followedBy(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);
```
            </td>
        </tr>
        <tr>
            <td><strong>followedByAny(#name)</strong></td>
            <td>
                <p>增加一个新的模式。可以有其他事件出现在匹配的事件和之前匹配到的事件中间，
                每个可选的匹配事件都会作为可选的匹配结果输出（不确定的松散连续）：</p>
```java
Pattern<Event, ?> followedByAny = start.followedByAny("middle");
```
             </td>
        </tr>
        <tr>
             <td><strong>followedByAny(#pattern_sequence)</strong></td>
             <td>
                 <p>增加一个新的模式。可以有其他事件出现在匹配的事件序列和之前匹配到的事件中间，
                 每个可选的匹配事件序列都会作为可选的匹配结果输出（不确定的松散连续）：</p>
```java
Pattern<Event, ?> followedByAny = start.followedByAny(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);
```
             </td>
        </tr>
        <tr>
                    <td><strong>notNext()</strong></td>
                    <td>
                        <p>增加一个新的否定模式。匹配的（否定）事件必须直接跟在前面匹配到的事件之后（严格连续）来丢弃这些部分匹配：</p>
```java
Pattern<Event, ?> notNext = start.notNext("not");
```
                    </td>
                </tr>
                <tr>
                    <td><strong>notFollowedBy()</strong></td>
                    <td>
                        <p>增加一个新的否定模式。即使有其他事件在匹配的（否定）事件和之前匹配的事件之间发生，
                        部分匹配的事件序列也会被丢弃（松散连续）：</p>
```java
Pattern<Event, ?> notFollowedBy = start.notFollowedBy("not");
```
                    </td>
                </tr>
       <tr>
          <td><strong>within(time)</strong></td>
          <td>
              <p>定义匹配模式的事件序列出现的最大时间间隔。如果未完成的事件序列超过了这个事件，就会被丢弃：</p>
```java
pattern.within(Time.seconds(10));
```
          </td>
       </tr>
  </tbody>
</table>
{{< /tab >}}
{{< tab "Scala" >}}
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">模式操作</th>
            <th class="text-center">描述</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><strong>begin(#name)</strong></td>
            <td>
            <p>定一个开始模式：</p>
```scala
val start = Pattern.begin[Event]("start")
```
            </td>
        </tr>
       <tr>
            <td><strong>begin(#pattern_sequence)</strong></td>
            <td>
            <p>定一个开始模式：</p>
```scala
val start = Pattern.begin(
    Pattern.begin[Event]("start").where(...).followedBy("middle").where(...)
)
```
            </td>
        </tr>
        <tr>
            <td><strong>next(#name)</strong></td>
            <td>
                <p>增加一个新的模式，匹配的事件必须是直接跟在前面匹配到的事件后面（严格连续）：</p>
```scala
val next = start.next("middle")
```
            </td>
        </tr>
        <tr>
            <td><strong>next(#pattern_sequence)</strong></td>
            <td>
                <p>增加一个新的模式。匹配的事件序列必须是直接跟在前面匹配到的事件后面（严格连续）：</p>
```scala
val next = start.next(
    Pattern.begin[Event]("start").where(...).followedBy("middle").where(...)
)
```
            </td>
        </tr>
        <tr>
            <td><strong>followedBy(#name)</strong></td>
            <td>
                <p>增加一个新的模式。可以有其他事件出现在匹配的事件和之前匹配到的事件中间（松散连续）：</p>
```scala
val followedBy = start.followedBy("middle")
```
            </td>
        </tr>
        <tr>
            <td><strong>followedBy(#pattern_sequence)</strong></td>
            <td>
                <p>增加一个新的模式。可以有其他事件出现在匹配的事件和之前匹配到的事件中间（松散连续）：</p>
```scala
val followedBy = start.followedBy(
    Pattern.begin[Event]("start").where(...).followedBy("middle").where(...)
)
```
            </td>
        </tr>
        <tr>
            <td><strong>followedByAny(#name)</strong></td>
            <td>
                <p>增加一个新的模式。可以有其他事件出现在匹配的事件和之前匹配到的事件中间，
                每个可选的匹配事件都会作为可选的匹配结果输出（不确定的松散连续）：</p>
```scala
val followedByAny = start.followedByAny("middle")
```
            </td>
         </tr>
         <tr>
             <td><strong>followedByAny(#pattern_sequence)</strong></td>
             <td>
                 <p>增加一个新的模式。可以有其他事件出现在匹配的事件序列和之前匹配到的事件中间，
                 每个可选的匹配事件序列都会作为可选的匹配结果输出（不确定的松散连续）：</p>
```scala
val followedByAny = start.followedByAny(
    Pattern.begin[Event]("start").where(...).followedBy("middle").where(...)
)
```
             </td>
         </tr>

                <tr>
                                    <td><strong>notNext()</strong></td>
                                    <td>
                                        <p>增加一个新的否定模式。匹配的（否定）事件必须直接跟在前面匹配到的事件之后
                                        （严格连续）来丢弃这些部分匹配：</p>
```scala
val notNext = start.notNext("not")
```
                                    </td>
                                </tr>
                                <tr>
                                    <td><strong>notFollowedBy()</strong></td>
                                    <td>
                                        <p>增加一个新的否定模式。即使有其他事件在匹配的（否定）事件和之前匹配的事件之间发生，
                                        部分匹配的事件序列也会被丢弃（松散连续）：</p>
```scala
val notFollowedBy = start.notFollowedBy("not")
```
                                    </td>
                                </tr>

       <tr>
          <td><strong>within(time)</strong></td>
          <td>
              <p>定义匹配模式的事件序列出现的最大时间间隔。如果未完成的事件序列超过了这个事件，就会被丢弃：</p>
```scala
pattern.within(Time.seconds(10))
```
          </td>
      </tr>
  </tbody>
</table>
{{< /tab >}}
{{< /tabs >}}

### 匹配后跳过策略

对于一个给定的模式，同一个事件可能会分配到多个成功的匹配上。为了控制一个事件会分配到多少个匹配上，你需要指定跳过策略`AfterMatchSkipStrategy`。
有五种跳过策略，如下：

* <strong>*NO_SKIP*</strong>: 每个成功的匹配都会被输出。
* <strong>*SKIP_TO_NEXT*</strong>: 丢弃以相同事件开始的所有部分匹配。
* <strong>*SKIP_PAST_LAST_EVENT*</strong>: 丢弃起始在这个匹配的开始和结束之间的所有部分匹配。
* <strong>*SKIP_TO_FIRST*</strong>: 丢弃起始在这个匹配的开始和第一个出现的名称为*PatternName*事件之间的所有部分匹配。
* <strong>*SKIP_TO_LAST*</strong>: 丢弃起始在这个匹配的开始和最后一个出现的名称为*PatternName*事件之间的所有部分匹配。

注意当使用*SKIP_TO_FIRST*和*SKIP_TO_LAST*策略时，需要指定一个合法的*PatternName*.

例如，给定一个模式`b+ c`和一个数据流`b1 b2 b3 c`，不同跳过策略之间的不同如下：

<table class="table table-bordered">
    <tr>
        <th class="text-left" style="width: 25%">跳过策略</th>
        <th class="text-center" style="width: 25%">结果</th>
        <th class="text-center"> 描述</th>
    </tr>
    <tr>
        <td><strong>NO_SKIP</strong></td>
        <td>
            <code>b1 b2 b3 c</code><br>
            <code>b2 b3 c</code><br>
            <code>b3 c</code><br>
        </td>
        <td>找到匹配<code>b1 b2 b3 c</code>之后，不会丢弃任何结果。</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_NEXT</strong></td>
        <td>
            <code>b1 b2 b3 c</code><br>
            <code>b2 b3 c</code><br>
            <code>b3 c</code><br>
        </td>
        <td>找到匹配<code>b1 b2 b3 c</code>之后，不会丢弃任何结果，因为没有以<code>b1</code>开始的其他匹配。</td>
    </tr>
    <tr>
        <td><strong>SKIP_PAST_LAST_EVENT</strong></td>
        <td>
            <code>b1 b2 b3 c</code><br>
        </td>
        <td>找到匹配<code>b1 b2 b3 c</code>之后，会丢弃其他所有的部分匹配。</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_FIRST</strong>[<code>b</code>]</td>
        <td>
            <code>b1 b2 b3 c</code><br>
            <code>b2 b3 c</code><br>
            <code>b3 c</code><br>
        </td>
        <td>找到匹配<code>b1 b2 b3 c</code>之后，会尝试丢弃所有在<code>b1</code>之前开始的部分匹配，但没有这样的匹配，所以没有任何匹配被丢弃。</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_LAST</strong>[<code>b</code>]</td>
        <td>
            <code>b1 b2 b3 c</code><br>
            <code>b3 c</code><br>
        </td>
        <td>找到匹配<code>b1 b2 b3 c</code>之后，会尝试丢弃所有在<code>b3</code>之前开始的部分匹配，有一个这样的<code>b2 b3 c</code>被丢弃。</td>
    </tr>
</table>

在看另外一个例子来说明NO_SKIP和SKIP_TO_FIRST之间的差别：
模式： `(a | b | c) (b | c) c+.greedy d`，输入：`a b c1 c2 c3 d`，结果将会是：


<table class="table table-bordered">
    <tr>
        <th class="text-left" style="width: 25%">跳过策略</th>
        <th class="text-center" style="width: 25%">结果</th>
        <th class="text-center"> 描述</th>
    </tr>
    <tr>
        <td><strong>NO_SKIP</strong></td>
        <td>
            <code>a b c1 c2 c3 d</code><br>
            <code>b c1 c2 c3 d</code><br>
            <code>c1 c2 c3 d</code><br>
        </td>
        <td>找到匹配<code>a b c1 c2 c3 d</code>之后，不会丢弃任何结果。</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_FIRST</strong>[<code>c*</code>]</td>
        <td>
            <code>a b c1 c2 c3 d</code><br>
            <code>c1 c2 c3 d</code><br>
        </td>
        <td>找到匹配<code>a b c1 c2 c3 d</code>之后，会丢弃所有在<code>c1</code>之前开始的部分匹配，有一个这样的<code>b c1 c2 c3 d</code>被丢弃。</td>
    </tr>
</table>

为了更好的理解NO_SKIP和SKIP_TO_NEXT之间的差别，看下面的例子：
模式：`a b+`，输入：`a b1 b2 b3`，结果将会是：


<table class="table table-bordered">
    <tr>
        <th class="text-left" style="width: 25%">跳过策略</th>
        <th class="text-center" style="width: 25%">结果</th>
        <th class="text-center"> 描述</th>
    </tr>
    <tr>
        <td><strong>NO_SKIP</strong></td>
        <td>
            <code>a b1</code><br>
            <code>a b1 b2</code><br>
            <code>a b1 b2 b3</code><br>
        </td>
        <td>找到匹配<code>a b1</code>之后，不会丢弃任何结果。</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_NEXT</strong></td>
        <td>
            <code>a b1</code><br>
        </td>
        <td>找到匹配<code>a b1</code>之后，会丢弃所有以<code>a</code>开始的部分匹配。这意味着不会产生<code>a b1 b2</code>和<code>a b1 b2 b3</code>了。</td>
    </tr>
</table>

想指定要使用的跳过策略，只需要调用下面的方法创建`AfterMatchSkipStrategy`：
<table class="table table-bordered">
    <tr>
        <th class="text-left" width="25%">方法</th>
        <th class="text-center">描述</th>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.noSkip()</code></td>
        <td>创建<strong>NO_SKIP</strong>策略</td>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.skipToNext()</code></td>
        <td>创建<strong>SKIP_TO_NEXT</strong>策略</td>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.skipPastLastEvent()</code></td>
        <td>创建<strong>SKIP_PAST_LAST_EVENT</strong>策略</td>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.skipToFirst(patternName)</code></td>
        <td>创建引用模式名称为<i>patternName</i>的<strong>SKIP_TO_FIRST</strong>策略</td>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.skipToLast(patternName)</code></td>
        <td>创建引用模式名称为<i>patternName</i>的<strong>SKIP_TO_LAST</strong>策略</td>
    </tr>
</table>

可以通过调用下面方法将跳过策略应用到模式上：

{{< tabs "e7240356-0fda-4a20-8b5a-7e4136753eca" >}}
{{< tab "Java" >}}
```java
AfterMatchSkipStrategy skipStrategy = ...
Pattern.begin("patternName", skipStrategy);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val skipStrategy = ...
Pattern.begin("patternName", skipStrategy)
```
{{< /tab >}}
{{< /tabs >}}

{% warn 注意 %} 使用SKIP_TO_FIRST/LAST时，有两个选项可以用来处理没有事件可以映射到对应的变量名上的情况。
默认情况下会使用NO_SKIP策略，另外一个选项是抛出异常。
可以使用如下的选项：

{{< tabs "48a6f23b-1861-4350-894d-0404d070cfb2" >}}
{{< tab "Java" >}}
```java
AfterMatchSkipStrategy.skipToFirst(patternName).throwExceptionOnMiss()
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
AfterMatchSkipStrategy.skipToFirst(patternName).throwExceptionOnMiss()
```
{{< /tab >}}
{{< /tabs >}}

## 检测模式

在指定了要寻找的模式后，该把它们应用到输入流上来发现可能的匹配了。为了在事件流上运行你的模式，需要创建一个`PatternStream`。
给定一个输入流`input`，一个模式`pattern`和一个可选的用来对使用事件时间时有同样时间戳或者同时到达的事件进行排序的比较器`comparator`，
你可以通过调用如下方法来创建`PatternStream`：

{{< tabs "c412e6ab-033c-496c-b72f-b351c056e365" >}}
{{< tab "Java" >}}
```java
DataStream<Event> input = ...
Pattern<Event, ?> pattern = ...
EventComparator<Event> comparator = ... // 可选的

PatternStream<Event> patternStream = CEP.pattern(input, pattern, comparator);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input : DataStream[Event] = ...
val pattern : Pattern[Event, _] = ...
var comparator : EventComparator[Event] = ... // 可选的

val patternStream: PatternStream[Event] = CEP.pattern(input, pattern, comparator)
```
{{< /tab >}}
{{< /tabs >}}

输入流根据你的使用场景可以是*keyed*或者*non-keyed*。

{% warn 注意 %} 在*non-keyed*流上使用模式将会使你的作业并发度被设为1。

### 从模式中选取

在获得到一个`PatternStream`之后，你可以应用各种转换来发现事件序列。推荐使用`PatternProcessFunction`。

`PatternProcessFunction`有一个`processMatch`的方法在每找到一个匹配的事件序列时都会被调用。
它按照`Map<String, List<IN>>`的格式接收一个匹配，映射的键是你的模式序列中的每个模式的名称，值是被接受的事件列表（`IN`是输入事件的类型）。
模式的输入事件按照时间戳进行排序。为每个模式返回一个接受的事件列表的原因是当使用循环模式（比如`oneToMany()`和`times()`）时，
对一个模式会有不止一个事件被接受。

```java
class MyPatternProcessFunction<IN, OUT> extends PatternProcessFunction<IN, OUT> {
    @Override
    public void processMatch(Map<String, List<IN>> match, Context ctx, Collector<OUT> out) throws Exception;
        IN startEvent = match.get("start").get(0);
        IN endEvent = match.get("end").get(0);
        out.collect(OUT(startEvent, endEvent));
    }
}
```

`PatternProcessFunction`可以访问`Context`对象。有了它之后，你可以访问时间属性，比如`currentProcessingTime`或者当前匹配的`timestamp`
（最新分配到匹配上的事件的时间戳）。
更多信息可以看[时间上下文](#时间上下文)。
通过这个上下文也可以将结果输出到[侧输出]({{< ref "docs/dev/datastream/side_output" >}}).


#### 处理超时的部分匹配

当一个模式上通过`within`加上窗口长度后，部分匹配的事件序列就可能因为超过窗口长度而被丢弃。可以使用`TimedOutPartialMatchHandler`接口
来处理超时的部分匹配。这个接口可以和其它的混合使用。也就是说你可以在自己的`PatternProcessFunction`里另外实现这个接口。
`TimedOutPartialMatchHandler`提供了另外的`processTimedOutMatch`方法，这个方法对每个超时的部分匹配都会调用。

```java
class MyPatternProcessFunction<IN, OUT> extends PatternProcessFunction<IN, OUT> implements TimedOutPartialMatchHandler<IN> {
    @Override
    public void processMatch(Map<String, List<IN>> match, Context ctx, Collector<OUT> out) throws Exception;
        ...
    }

    @Override
    public void processTimedOutMatch(Map<String, List<IN>> match, Context ctx) throws Exception;
        IN startEvent = match.get("start").get(0);
        ctx.output(outputTag, T(startEvent));
    }
}
```

<span class="label label-info">Note</span> `processTimedOutMatch`不能访问主输出。
但你可以通过`Context`对象把结果输出到[侧输出]({{< ref "docs/dev/datastream/side_output" >}})。


#### 便捷的API

前面提到的`PatternProcessFunction`是在Flink 1.8之后引入的，从那之后推荐使用这个接口来处理匹配到的结果。
用户仍然可以使用像`select`/`flatSelect`这样旧格式的API，它们会在内部被转换为`PatternProcessFunction`。

{{< tabs "3f1bfda2-5b78-4b1f-bb3d-79e31527c912" >}}
{{< tab "Java" >}}

```java
PatternStream<Event> patternStream = CEP.pattern(input, pattern);

OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

SingleOutputStreamOperator<ComplexEvent> flatResult = patternStream.flatSelect(
    outputTag,
    new PatternFlatTimeoutFunction<Event, TimeoutEvent>() {
        public void timeout(
                Map<String, List<Event>> pattern,
                long timeoutTimestamp,
                Collector<TimeoutEvent> out) throws Exception {
            out.collect(new TimeoutEvent());
        }
    },
    new PatternFlatSelectFunction<Event, ComplexEvent>() {
        public void flatSelect(Map<String, List<IN>> pattern, Collector<OUT> out) throws Exception {
            out.collect(new ComplexEvent());
        }
    }
);

DataStream<TimeoutEvent> timeoutFlatResult = flatResult.getSideOutput(outputTag);
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala

val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

val outputTag = OutputTag[String]("side-output")

val result: SingleOutputStreamOperator[ComplexEvent] = patternStream.flatSelect(outputTag){
    (pattern: Map[String, Iterable[Event]], timestamp: Long, out: Collector[TimeoutEvent]) =>
        out.collect(TimeoutEvent())
} {
    (pattern: mutable.Map[String, Iterable[Event]], out: Collector[ComplexEvent]) =>
        out.collect(ComplexEvent())
}

val timeoutResult: DataStream[TimeoutEvent] = result.getSideOutput(outputTag)
```

{{< /tab >}}
{{< /tabs >}}

## CEP库中的时间

### 按照事件时间处理迟到事件

在`CEP`中，事件的处理顺序很重要。在使用事件时间时，为了保证事件按照正确的顺序被处理，一个事件到来后会先被放到一个缓冲区中，
在缓冲区里事件都按照时间戳从小到大排序，当水位线到达后，缓冲区中所有小于水位线的事件被处理。这意味着水位线之间的数据都按照时间戳被顺序处理。

{% warn 注意 %} 这个库假定按照事件时间时水位线一定是正确的。

为了保证跨水位线的事件按照事件时间处理，Flink CEP库假定*水位线一定是正确的*，并且把时间戳小于最新水位线的事件看作是*晚到*的。
晚到的事件不会被处理。你也可以指定一个侧输出标志来收集比最新水位线晚到的事件，你可以这样做：

{{< tabs "b574545b-666f-44eb-87d2-0dedff6bc3aa" >}}
{{< tab "Java" >}}

```java
PatternStream<Event> patternStream = CEP.pattern(input, pattern);

OutputTag<String> lateDataOutputTag = new OutputTag<String>("late-data"){};

SingleOutputStreamOperator<ComplexEvent> result = patternStream
    .sideOutputLateData(lateDataOutputTag)
    .select(
        new PatternSelectFunction<Event, ComplexEvent>() {...}
    );

DataStream<String> lateData = result.getSideOutput(lateDataOutputTag);

```

{{< /tab >}}
{{< tab "Scala" >}}

```scala

val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

val lateDataOutputTag = OutputTag[String]("late-data")

val result: SingleOutputStreamOperator[ComplexEvent] = patternStream
      .sideOutputLateData(lateDataOutputTag)
      .select{
          pattern: Map[String, Iterable[ComplexEvent]] => ComplexEvent()
      }

val lateData: DataStream[String] = result.getSideOutput(lateDataOutputTag)

```

{{< /tab >}}
{{< /tabs >}}

### 时间上下文

在[PatternProcessFunction](#从模式中选取)中，用户可以和[IterativeCondition](#条件)中
一样按照下面的方法使用实现了`TimeContext`的上下文：

```java
/**
 * 支持获取事件属性比如当前处理事件或当前正处理的事件的时间。
 * 用在{@link PatternProcessFunction}和{@link org.apache.flink.cep.pattern.conditions.IterativeCondition}中
 */
@PublicEvolving
public interface TimeContext {

	/**
	 * 当前正处理的事件的时间戳。
	 *
	 * <p>如果是{@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}，这个值会被设置为事件进入CEP算子的时间。
	 */
	long timestamp();

	/** 返回当前的处理时间。 */
	long currentProcessingTime();
}
```

这个上下文让用户可以获取处理的事件（在`IterativeCondition`时候是进来的记录，在`PatternProcessFunction`是匹配的结果）的时间属性。
调用`TimeContext#currentProcessingTime`总是返回当前的处理时间，而且尽量去调用这个函数而不是调用其它的比如说`System.currentTimeMillis()`。

使用`EventTime`时，`TimeContext#timestamp()`返回的值等于分配的时间戳。
使用`ProcessingTime`时，这个值等于事件进入CEP算子的时间点（在`PatternProcessFunction`中是匹配产生的时间）。
这意味着多次调用这个方法得到的值是一致的。

## 例子

下面的例子在一个分片的`Events`流上检测模式`start, middle(name = "error") -> end(name = "critical")`。
事件按照`id`分片，一个有效的模式需要发生在10秒内。

{{< tabs "01929551-b785-41f4-ab0d-b6369ce3cc41" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = ...

DataStream<Event> input = ...

DataStream<Event> partitionedInput = input.keyBy(new KeySelector<Event, Integer>() {
	@Override
	public Integer getKey(Event value) throws Exception {
		return value.getId();
	}
});

Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
	.next("middle").where(new SimpleCondition<Event>() {
		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("error");
		}
	}).followedBy("end").where(new SimpleCondition<Event>() {
		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("critical");
		}
	}).within(Time.seconds(10));

PatternStream<Event> patternStream = CEP.pattern(partitionedInput, pattern);

DataStream<Alert> alerts = patternStream.select(new PatternSelectFunction<Event, Alert>() {
	@Override
	public Alert select(Map<String, List<Event>> pattern) throws Exception {
		return createAlert(pattern);
	}
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env : StreamExecutionEnvironment = ...

val input : DataStream[Event] = ...

val partitionedInput = input.keyBy(event => event.getId)

val pattern = Pattern.begin[Event]("start")
  .next("middle").where(_.getName == "error")
  .followedBy("end").where(_.getName == "critical")
  .within(Time.seconds(10))

val patternStream = CEP.pattern(partitionedInput, pattern)

val alerts = patternStream.select(createAlert(_))
```
{{< /tab >}}
{{< /tabs >}}

## 从旧版本迁移（1.5之前）

### 从旧版本迁移Flink <= 1.5

在Flink 1.13中，我们放弃了与Flink <= 1.5直接向后兼容的`savepoint`。如果想从旧版本的`savepoint`恢复，请先将其迁移到Flink(1.6-1.12)，在该版本下生成一个`savepoint`，然后升级至Flink >= 1.13，再使用该`savepoint`使用进行恢复。

{{< top >}}

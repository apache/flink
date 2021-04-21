---
title: "Custom Serialization for Managed State"
nav-title: "自定义状态序列化"
nav-parent_id: streaming_state
nav-pos: 7
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

* ToC
{:toc}

本页面为需要自定义序列化器的用户提供指导，包括如何自定义序列化器，以及实现支持状态升级的序列化器的最佳指南。

如果你仅使用 Flink 自带的序列化器，可以忽略本节内容。

## 使用自定义序列化器
注册 Managed keyed/operator state 的时候，需要提供一个包含 state 名字以及其 state 类型信息的 `StateDescriptor`。Flink 的 [type serialization framework](../../types_serialization.html) 会使用类型信息创建对应的序列化器。

当然也可以通过传递自定义的 `TypeSerializer` 给 `StateDescriptor`，让 Flink 使用自定义序列化器序列化 managed state：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class CustomTypeSerializer extends TypeSerializer<Tuple2<String, Integer>> {...};

ListStateDescriptor<Tuple2<String, Integer>> descriptor =
    new ListStateDescriptor<>(
        "state-name",
        new CustomTypeSerializer());

checkpointedState = getRuntimeContext().getListState(descriptor);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class CustomTypeSerializer extends TypeSerializer[(String, Integer)] {...}

val descriptor = new ListStateDescriptor[(String, Integer)](
    "state-name",
    new CustomTypeSerializer)
)

checkpointedState = getRuntimeContext.getListState(descriptor)
{% endhighlight %}
</div>
</div>

## State serializers and schema evolution

本节介绍了 state 序列化以及结构升级相关的面向用户的抽象，以及 Flink 如何与这些抽象交互的一些内部细节。

从 savepoint 恢复时，Flink 允许更改 state 的序列化器，从而支持结构升级。state 恢复之后，将使用新的序列化器进行 state 注册（即恢复后作业中 `StateDescriptor` 指定的序列化器），
新的序列化器可能和之前的序列化器拥有不同的结构。因此，实现 state 序列化器的时候，处理正确处理读写数据的基本逻辑外，另外一个需要重点考虑的是未来如何支持 state 的机构升级。

这里说的 *结构*，既可能指 state 的 *数据模型*，也可能指 *序列化之后的二进制格式"。一般来说，结构在如下情况下会发生改变：

 1. state 的结构发生变化，比如 POJO 类中增加或删除字段。
 2. 一般来说，数据格式变化之后，序列化器的序列化格式需要进行升级。
 3. 序列化器的配置发生了变化。
 
为了能在新作业中获取到之前 state 的结构，并检测到模式是否发生变化，在生成 savepoint 的时候，会把 state 序列化器的快照也一并写出。这个快照被抽象为 `TypeSerializerSnapshot`，在下一节中详细描述

### The `TypeSerializerSnapshot` abstraction

<div data-lang="java" markdown="1">
{% highlight java %}
public interface TypeSerializerSnapshot<T> {
    int getCurrentVersion();
    void writeSnapshot(DataOuputView out) throws IOException;
    void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException;
    TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer);
    TypeSerializer<T> restoreSerializer();
}
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
public abstract class TypeSerializer<T> {    
    
    // ...
    
    public abstract TypeSerializerSnapshot<T> snapshotConfiguration();
}
{% endhighlight %}
</div>
序列化器的 `TypeSerializerSnapshot` 包含序列化器的结构信息，以及恢复序列化器所需要的其他附加信息。序列化器的快照读写逻辑在 `writeSnapshot` 以及 `readSnapshot` 中进行实现。

需要注意的是快照本身的格式可能也需要随时间发生变化（比如，往快照中增加更多序列化器的信息）。为了方便，快照携带版本号，可以通过 `getCurrentVersion` 获取当前版本。在恢复的时候，从 savepoint 读取到快照后，`readSnapshot` 会调用对应版本的实现方法。

在恢复时，检测序列化器格式是否发生变化的逻辑应该在 `resolveSchemaCompatibility` 中实现，该方法接收新的序列化器作为参数。该方法返回一个 `TypeSerializerSchemaCompatibility` 表示兼容性的结果，该结果有如下几种：
 1. **`TypeSerializerSchemaCompatibility.compatibleAsIs()`**: 该结果表明新的序列化器是兼容的，意味着新序列化器和之前的序列化器拥有相同的格式。也可能是在 `resolveSchemaCompatibility` 中对新序列化器继续重新配置所达到的。
 2. **`TypeSerializerSchemaCompatibility.compatibleAfterMigration()`**: 该结果表示新旧序列化器的格式不同，不过可以使用之前的序列化器反序列化，然后再用新序列化器进行序列化，从而进行迁移。
 3. **`TypeSerializerSchemaCompatibility.incompatible()`**: 该结果表示新旧序列化器的格式不同，且无法进行迁移。

最后一点细节在于需要进行 state 迁移时如何获取之前的序列化器。`TypeSerializerSnapshot` 的另一个重要作用是可以构造之前的序列化器。更具体的说，`TypeSerializerSnapshot` 应该实现 `restoreSerializer` 方法，该方法返回一个可以安全读取之前序列化器写出数据的序列化器。

### How Flink interacts with the `TypeSerializer` and `TypeSerializerSnapshot` abstractions

总结一下，本节阐述了 Flink，或者更具体的说状态后端，是如何与这些抽象交互。根据状态后端的不同，交互方式略有不同，但这与序列化器以及序列化器快照的实现是正交的。

#### Off-heap state backends (e.g. `RocksDBStateBackend`)

 1. **以拥有格式 _A_ 的序列化器注册一个新的 state**
  - state 的每次访问（读/写）都使用注册的 `TypeSerializer`.
  - state 以格式 *A* 进行序列化.
 2. **Take a savepoint**
  - 通过 `TypeSerializer#snapshotConfiguration` 获取序列化器快照。
  - 序列化器快照和序列化后的 state 数据一起写到 savepoin。
 3. **以一个拥有格式 _B_ 的新序列化器恢复 state 数据**
  - 恢复之前序列化器的快照。
  - state 的数据不会真正被反序列化，仅加载到状态后端（这个时候仍然以格式 *A* 的形式存在）。
  - 接受到新序列化器之后，会通过 `TypeSerializer#resolveSchemaCompatibility` 检查格式的兼容性。
 4. **在状态后端中从格式 _A_ 迁移到格式 _B_**
  - 如果格式发生了变化且能够进行迁移，则会迁移。通过 `TypeSerializerSnapshot#restoreSerializer()` 获取之前格式 _A_ 的序列化器，用于反序列化 state 数据，然后使用格式 _B_ 的序列化器序列化，从而完成迁移。迁移工作会在其他工作之前一次性完成。
  - 如果格式不兼容，则会抛异常。
 
#### Heap state backends (e.g. `MemoryStateBackend`, `FsStateBackend`)

 1. **以拥有格式 _A_ 的序列化器注册一个新的 state**
  - 注册的 `TypeSerializer` 后状态后端维护.
 2. **做 savepoint 时, 以格式 _A_ 序列化所有的 state**
  - 通过 `TypeSerializer#snapshotConfiguration` 获取序列化器快照.
  - 将序列化器快照写到 savepoint.
  - 以格式 _A_ 将 state 序列化到 savepoint 中。
 3. **恢复时，将 state 反序列化到堆上的对象**
  - 恢复之前的序列化器快照。
  - 通过 `TypeSerializerSnapshot#restoreSerializer()` 获取之前的格式 _A_ 的序列化器，并用于反序列化 state。
  - 至此，所有的 state 都被反序列化完成。
 4. **恢复后以新格式 _B_ 的序列化器访问 state。
  - 接受到新的序列化器后，通过 `TypeSerializer#resolveSchemaCompatibility` 检查格式的兼容性。
  - 如果兼容性表明需要迁移，则不需要做任何事情，因为所有的 state 都已经被反序列化成对象。
  - 如果兼容性表明不兼容，则会抛出异常。
 5. **再次执行 savepoint 时，以格式 _B_ 序列化所有的 state**
  - 和第二步一样，不过使用格式 _B_。

## Predefined convenient `TypeSerializerSnapshot` classes

Flink 提供了 `TypeSerializerSnapshot` 在两个典型场景下的抽象类 `SimpleTypeSerializerSnapshot` 和 `CompositeTypeSerializerSnapshot`。

使用这些预定义快照类的序列化器需要提供自己独立的实现。这是不同序列化器之间不共享快照类的最佳实践，这一点在下一节有更详细的描述。

### Implementing a `SimpleTypeSerializerSnapshot`

`SimpleTypeSerializerSnapshot` 用于那些没有任何状态或者配置的序列化器，基本意味着序列化器的格式由序列化器的类所决定。

使用 `SimpleTypeSerializerSnapshot` 的情况下，格式兼容性的结果只有两种：
 - `TypeSerializerSchemaCompatibility.compatibleAsIs()`：新旧序列化器完全兼容，或者
 - `TypeSerializerSchemaCompatibility.incompatible()`：新旧序列化器完全不兼容。

下面是以 `IntSerializer` 为例的一个介绍 `SimpleTypeSerializerSnapshot` 的例子：
<div data-lang="java" markdown="1">
{% highlight java %}
public class IntSerializerSnapshot extends SimpleTypeSerializerSnapshot<Integer> {
    public IntSerializerSnapshot() {
        super(() -> IntSerializer.INSTANCE);
    }
}
{% endhighlight %}
</div>

`IntSerializer` 没有任何状态或配置。序列化器的格式完全有序列化器类所决定，只能被另外一个 `IntSerializer` 所读取。因此很适合 `SimpleTypeSerializerSnapshot`。

`SimpleTypeSerializerSnapshot` 的构造函数需要一个提供对应序列化器的 `Supplier`, 这个序列化器会在做快照或者恢复时使用。在恢复时会检查 supplier 返回的序列化器是否是预期内的序列化器类。

### Implementing a `CompositeTypeSerializerSnapshot`

`CompositeTypeSerializerSnapshot` 主要用于那些依赖多个嵌套序列化器的组合序列化器。

在进一步解释之前，我们将依赖多个嵌套序列化器的序列化器称为"外部"序列化器。比如 `MapSerializer`, `ListSerializer` 以及 `GenericArraySerializer` 等。以 `MapSerializer` 为例 -- 键和值的序列化器都是嵌套序列化器，而 `MapSerializer` 本身就是 "外部" 序列化器。

这种情况下，外部序列化器的快照还应该包含嵌套序列化器的快照，从而可以单独检查嵌套序列化器的兼容性。嵌套序列化器的兼容性会影响外部序列化器的兼容性。

`CompositeTypeSerializerSnapshot` 用于实现这类符合序列化器的快照。它覆盖了嵌套序列化器快照的读写，以及考虑所有嵌套序列化器的兼容性来决定外部序列化器的兼容性。 

下面以 `MapSerializer` 为例来阐述如何使用 `CompositeTypeSerializerSnapshot`。
<div data-lang="java" markdown="1">
{% highlight java %}
public class MapSerializerSnapshot<K, V> extends CompositeTypeSerializerSnapshot<Map<K, V>, MapSerializer> {

    private static final int CURRENT_VERSION = 1;

    public MapSerializerSnapshot() {
        super(MapSerializer.class);
    }

    public MapSerializerSnapshot(MapSerializer<K, V> mapSerializer) {
        super(mapSerializer);
    }

    @Override
    public int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @Override
    protected MapSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
        TypeSerializer<K> keySerializer = (TypeSerializer<K>) nestedSerializers[0];
        TypeSerializer<V> valueSerializer = (TypeSerializer<V>) nestedSerializers[1];
        return new MapSerializer<>(keySerializer, valueSerializer);
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(MapSerializer outerSerializer) {
        return new TypeSerializer<?>[] { outerSerializer.getKeySerializer(), outerSerializer.getValueSerializer() };
    }
}
{% endhighlight %}
</div>

当作为 `CompositeTypeSerializerSnapshot` 的子类实现新序列化器快照时，必须实现如下三个方法：
 * `#getCurrentOuterSnapshotVersion()`: 返回外部序列化器序列化快照的当前版本
 * `#getNestedSerializers(TypeSerializer)`: 返回外部序列化器的嵌套序列化器
 * `#createOuterSerializerWithNestedSerializers(TypeSerializer[])`: 创建一个包含给定嵌套序列化器的外部序列化器

上述示例是一个除嵌套序列化器快照外，没有其他额外信息的 `CompositeTypeSerializerSnapshot` 例子。因此，外部序列化器永远不会有升级需求。而其他一些序列化器，则包含了一些需要和嵌套序列化器快照一起被持久化的静态配置，Flink 的 `GenericArraySerializer` 是其中一个例子，除嵌套序列化器的信息外，还包含了数组元素类型的相关信息。

这种情况下，还需要实现其他三个方法：
 * `#writeOuterSnapshot(DataOutputView)`: 定义如何持久化外部序列化器快照信息
 * `#readOuterSnapshot(int, DataInputView, ClassLoader)`: 定义如何读取外部序列化器快照信息
 * `#resolveOuterSchemaCompatibility(TypeSerializer)`: 根据外部序列化器快照的相关信息检查兼容性

默认情况下，`CompositeTypeSerializerSnapshot` 假定外部序列化器没有任何快照信息需要读写，因此上述三个方法均是空实现。如果子类有相应需求，则上述三个方法均需实现。

下面以 `GenericArraySerializer` 为例，介绍拥有外部快照信息的序列化器如何使用 `CompositeTypeSerializerSnapshot`。

<div data-lang="java" markdown="1">
{% highlight java %}
public final class GenericArraySerializerSnapshot<C> extends CompositeTypeSerializerSnapshot<C[], GenericArraySerializer> {

    private static final int CURRENT_VERSION = 1;

    private Class<C> componentClass;

    public GenericArraySerializerSnapshot() {
        super(GenericArraySerializer.class);
    }

    public GenericArraySerializerSnapshot(GenericArraySerializer<C> genericArraySerializer) {
        super(genericArraySerializer);
        this.componentClass = genericArraySerializer.getComponentClass();
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @Override
    protected void writeOuterSnapshot(DataOutputView out) throws IOException {
        out.writeUTF(componentClass.getName());
    }

    @Override
    protected void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        this.componentClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
    }

    @Override
    protected boolean resolveOuterSchemaCompatibility(GenericArraySerializer newSerializer) {
        return (this.componentClass == newSerializer.getComponentClass())
            ? OuterSchemaCompatibility.COMPATIBLE_AS_IS
            : OuterSchemaCompatibility.INCOMPATIBLE;
    }

    @Override
    protected GenericArraySerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
        TypeSerializer<C> componentSerializer = (TypeSerializer<C>) nestedSerializers[0];
        return new GenericArraySerializer<>(componentClass, componentSerializer);
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(GenericArraySerializer outerSerializer) {
        return new TypeSerializer<?>[] { outerSerializer.getComponentSerializer() };
    }
}
{% endhighlight %}
</div>

上述代码片段中，有两个重要的事情需要注意。首先，外部序列化器的额外信息会被持久化到快照中，因此当外部序列化器的额外信息被修改后，`getCurrentOuterSnapshotVersion()` 的返回值也需要随之升级。

另外就是，注意我们如何避免使用 Java 的序列化器，仅持久化类名，并在恢复时进行动态加载。避免使用 Java 的序列化器通常是一个很好的做法，有关这一点的详细描述将在下一节介绍。

## 最佳实践

#### 1. Flink restores serializer snapshots by instantiating them with their classname

序列化器的快照，是如何序列化 state 的唯一事实标准。为了能够恢复并访问之前的 state，之前序列化器的快照也必须进行恢复。

Flink 会使用类名实例化具体的 `TypeSerializerSnapshot`（类名会和快照一起写出）。因此，为了防止无意的类名改动或者初始化 `TypeSerializerSnapshot` 失败，需要保证：
 - 不能以匿名类或者内部类的形式实现
 - 有一个无参的 public 构造函数

#### 2. Avoid sharing the same `TypeSerializerSnapshot` class across different serializers

由于格式兼容检查是通过序列化器快照进行的，因此多个序列化器返回同一个 `TypeSerializerSnapshot` 会导致 `TypeSerializerSnapshot#resolveSchemaCompatibility` 和 `TypeSerializerSnapshot#restoreSerializer()` 的实现变复杂。

共用 `TypeSerializerSnapshot` 从分工来说也是不好的选择，每个序列化器的的格式，配置信息以及如何恢复等都应该放到一个单独的 `TypeSerializerSnapshot` 中。

#### 3. Avoid using Java serialization for serializer snapshot content

应该避免使用 Java 序列化器序列化序列化器快照。举例来说，序列化器在序列化一个目标类的时候，应该序列化类名，而不是直接序列化该对象。恢复快照时，通过读取类名，然后动态加载生成具体的序列化器快照对象。

这种方式确保了序列化器快照始终可以被安全地读取。在上面的例子中，如果通过 Java 序列化器把类序列化，那么一旦序列化器快照类的实现进行了修改，会由于二进制兼容问题导致无法读取。

## Migrating from deprecated serializer snapshot APIs before Flink 1.7

本节将介绍如何从 Flink 1.7 之前的序列化器以及序列化器快照 API 进行迁移。

在 Flink 1.7 之前，序列化器快照是以 `TypeSerializerConfigSnapshot` 存在（现在已经过时，会在未来被 `TypeSerializerSnapshot` 完全取代并删除）。此外，序列化器格式的兼容性检查在 `TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)` 中执行。

新旧实现的另一个主要区别是，`TypeSerializerConfigSnapshot` 无法实例化之前的序列化器。因此，序列化器仍然需要返回一个 `TypeSerializerConfigSnapshot` 子类作为快照，序列化器对象本身会通过 Java 序列化器写到 savepoint 中，这样恢复时可能可以从 savepoint 中恢复原来的序列化器。这是非常不可取的，因为作业是否能够恢复成功，取决于能否从 savepoint 正确恢复原来的序列化器。这限制了序列化器的升级，一旦序列化器继续升级就可能出问题。

为将来序列化器能够能够升级，强烈建议迁移使用新 API，具体操作步骤如下所示：
 1. 实现 `TypeSerializerSnapshot` 的一个子类作为序列化器的快照类。
 2. 在调用 `TypeSerializer#snapshotConfiguration()` 时返回新的序列化器快照类
 3. 从就作业的 savepoint 恢复，并执行一个新的 savepoint。注意：这一步中旧的 `TypeSerializerConfigSnapshot` 必须存在于 classpath 中，并且 `TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)` 不能被删除。这个操作是希望将 savepoint 中的 `TypeSerializerConfigSnapshot` 替换成新的 `TypeSerializerSnapshot`。
 4. 新版本（>= 1.7) 的 savepoint 包含 `TypeSerializerSnapshot` 作为序列化器的快照，而且不再需要序列化序列化器对象到 savepoint 中。现在可以安全的删除 `TypeSerializerConfigSnapshot` 的子类以及 `TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)` 实现。

{% top %}

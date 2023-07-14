---
title: "自定义状态序列化"
weight: 8
type: docs
aliases:
  - /zh/dev/stream/state/custom_serialization.html
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

# 托管状态的自定义序列化


这个页面是为需要为其状态使用自定义序列化的用户提供的指南，包括如何提供自定义状态序列化器以及实现允许状态模式演进的序列化器的指南和最佳实践。

如果你仅使用 Flink 自己的序列化器，那么这个页面对你来说无关紧要，可以忽略。
## 使用自定义状态序列化器

在注册管理的操作符或键控状态时，需要一个`StateDescriptor`来指定状态的名称，以及关于状态类型的信息。类型信息被Flink的[类型序列化框架]({{< ref "docs/dev/datastream/fault-tolerance/serialization/types_serialization" >}})用来为状态创建合适的序列化器。

也可以完全绕过这个并让 Flink 使用您自己的自定义序列化器来序列化托管状态，
只需使用您自己的`TypeSerializer`实现直接实例化`StateDescriptor`即可：

{{< tabs "ee215ff6-2e21-4a40-a1b4-7f114560546f" >}}
{{< tab "Java" >}}

```java
public class CustomTypeSerializer extends TypeSerializer<Tuple2<String, Integer>> {...};

ListStateDescriptor<Tuple2<String, Integer>> descriptor =
    new ListStateDescriptor<>(
        "state-name",
        new CustomTypeSerializer());

checkpointedState = getRuntimeContext().getListState(descriptor);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class CustomTypeSerializer extends TypeSerializer[(String, Integer)] {...}

val descriptor = new ListStateDescriptor[(String, Integer)](
    "state-name",
    new CustomTypeSerializer)
)

checkpointedState = getRuntimeContext.getListState(descriptor)
```
{{< /tab >}}
{{< /tabs >}}

## 状态序列化器和模式演进

本节解释与状态序列化和模式演化相关的面向用户的抽象，以及必要的有关 Flink 如何与这些抽象交互的内部细节。

从保存点恢复时，Flink 允许更改用于读取和写入先前注册状态的序列化器，这样用户就不会被锁定到任何特定的序列化模式。 当状态恢复时，将出现一个新的序列化器注册状态（即，带有`StateDescriptor`的序列化器，用于访问状态恢复工作）。这个新的序列化器可能具有与以前的序列化器不同的架构。 因此，当实现状态序列化器，除了读/写数据的基本逻辑之外，另一个重要的事情是要记住考虑的是未来如何更改序列化模式。

当谈到*模式*时，在这种情况下，该术语可以在指状态的*数据模型*之间互换类型和状态类型的*序列化二进制格式*。 一般来说，该模式可能会在某些情况下发生变化：

1. 状态类型的数据模式已经发展，即从 POJO 中添加或删除用作状态的字段。
2. 一般来说，数据模式改变后，序列化器的序列化格式需要升级。
3. 序列化器的配置已经发生了变化。

为了让新的执行获得有关状态的`写入模式`的信息并检测是否模式已更改，在获取操作符状态的保存点时，需要状态序列化器的*快照*与状态字节一起写入。 这是抽象的`TypeSerializerSnapshot`，将在下一小节中进行解释。

### `TypeSerializerSnapshot` 抽象

```java
public interface TypeSerializerSnapshot<T> {
    int getCurrentVersion();
    void writeSnapshot(DataOuputView out) throws IOException;
    void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException;
    TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer);
    TypeSerializer<T> restoreSerializer();
}
```
```java
public abstract class TypeSerializer<T> {    
    
    // ...
    
    public abstract TypeSerializerSnapshot<T> snapshotConfiguration();
}
```

序列化器的`TypeSerializerSnapshot`是一个时间点信息，作为有关状态序列化器的写入模式，以及恢复序列化器所必需的任何附加信息
将与给定时间点相同。 关于恢复时应写入和读取内容的逻辑因为序列化器快照是在`writeSnapshot`和`readSnapshot`方法中定义的。

请注意，快照自己的写入模式也可能需要随着时间的推移而改变（例如，当您希望添加更多信息时关于快照的序列化器）。 为了促进这一点，快照被版本化，使用当前版本在`getCurrentVersion`方法中定义的数字。 恢复时，当从保存点读取序列化器快照时，
写入快照的架构版本将提供给`readSnapshot`方法，以便读取实现可以处理不同的版本。

在恢复时，检测新序列化器的模式是否已更改的逻辑应在`resolveSchemaCompatibility` 方法。 当以前的注册状态再次注册到新的序列化器时恢复操作符的执行，通过此方法将新的序列化器提供给先前序列化器的快照。该方法返回一个表示兼容性解析结果的`TypeSerializerSchemaCompatibility`，它可以是以下之一：

1. **`TypeSerializerSchemaCompatibility.compatibleAsIs()`**: 这个结果表明新的串行器是兼容的，这意味着新的序列化器与以前的序列化器具有相同的架构。 有可能新的序列化器已在`resolveSchemaCompatibility`方法中重新配置，以便它兼容。
2. **`TypeSerializerSchemaCompatibility.compatibleAfterMigration()`**:这个结果表明新的序列化器有一个不同的序列化模式，并且可以使用以前的序列化器从旧模式迁移（它识别旧模式）将字节读入状态对象，然后将对象重写回字节新的序列化器（它识别新的模式）。
3. **`TypeSerializerSchemaCompatibility.incompatible()`**: 这个结果表明新的串行器有一个不同的序列化模式，但无法从旧模式迁移。

最后一点细节是在需要迁移的情况下如何获取之前的序列化器。序列化器的`TypeSerializerSnapshot`的另一个重要作用是它充当恢复工厂之前的序列化器。 更具体地说，`TypeSerializerSnapshot`应该实现`restoreSerializer`方法实例化一个序列化器实例，该实例可识别先前序列化器的架构和配置，因此可以安全地读取原有序列化器写入的数据。

### Flink 如何与`TypeSerializer`和`TypeSerializerSnapshot`抽象交互

总而言之，本节总结了 Flink（或更具体地说状态后端）如何与抽象。 根据状态后端的不同，交互略有不同，但这是正交的状态序列化器及其序列化器快照的实现。

#### 堆外状态后端（例如`RocksDBStateBackend`）

1. **使用具有模式 _A_ 的状态序列化器注册新状态**
- 状态的已注册`TypeSerializer`用于在每次状态访问时读取/写入状态。
- 状态被写入模式 *A* 中。
2. **获取一个保存点**
- 序列化器快照是通过`TypeSerializer#snapshotConfiguration`方法获取的。
- 序列化器快照以及已序列化的状态字节（使用模式 *A*）写入保存点。
3. **恢复的执行使用具有模式 _B_ 的新状态序列化器重新访问恢复的状态字节**
- 原有状态序列化器的快照已恢复。
- 状态字节在恢复时不会反序列化，仅加载回状态后端（因此，仍处于模式 *A* 中）。
- 收到新的序列化器后，它会通过以下方式提供给恢复的先前序列化器的快照`TypeSerializer#resolveSchemaCompatibility` 用于检查架构兼容性。
4. **将后端的状态字节从模式 _A_ 迁移到模式 _B_**
- 如果兼容性解析反映模式已更改并且可以进行迁移，则模式可迁移。 识别模式 _A_ 的先前状态序列化器将从序列化器快照中获取，通过`TypeSerializerSnapshot#restoreSerializer()`，用于将状态字节反序列化为对象，而对象又使用新的序列化程序再次重写，该序列化程序可识别模式 _B_ 以完成迁移。 所有节点在处理继续之前，所访问的状态会全部迁移。
- 如果解决方案表明不兼容，则状态访问将失败并出现异常。

#### 堆状态后端（例如`MemoryStateBackend`、`FsStateBackend`）

1. **使用具有模式 _A_ 的状态序列化器注册新状态**
- 已注册的`TypeSerializer`由状态后端维护。
2. **采取保存点，使用模式 _A_ 序列化所有状态**
- The serializer snapshot is extracted via the`TypeSerializer#snapshotConfiguration` method.
- 序列化器快照是通过`TypeSerializer#snapshotConfiguration`方法获取的。
- 状态对象现在被序列化到保存点，以模式 _A_ 写入。
3. **恢复时，将状态反序列化为堆中的对象**
- 先前状态序列化器的快照已恢复。
- 先前的序列化程序可识别模式 _A_，是从序列化程序快照中获取的，通过`TypeSerializerSnapshot#restoreSerializer()`，用于将状态字节反序列化为对象。
- 从现在开始，所有的状态都已经反序列化了。
4. **恢复的执行使用具有模式 _B_ 的新状态序列化器重新访问以前的状态**
- 收到新的序列化器后，它会通过以下方式提供给恢复的先前序列化器的快照`TypeSerializer#resolveSchemaCompatibility` 用于检查架构兼容性。
- 如果兼容性检查表明需要迁移，则在这种情况下不会发生任何事情，因为 for堆后端，所有状态都已反序列化为对象。
- 如果解决方案表明不兼容，则状态访问将失败并出现异常。
5. **采取另一个保存点，使用模式 _B_ 序列化所有状态**
- 与步骤 2 相同，但现在状态字节全部位于模式 _B_ 中。

## 预定义的方便的`TypeSerializerSnapshot`类

Flink 提供了两个抽象基`TypeSerializerSnapshot` 类，可用于典型场景：`SimpleTypeSerializerSnapshot` 和`CompositeTypeSerializerSnapshot`。

提供这些预定义快照作为其序列化器快照的序列化器必须始终拥有自己的独立快照子类实现。 这对应于不共享快照类的最佳实践跨不同的序列化器，下一节将对此进行更彻底的解释。

### 实现`SimpleTypeSerializerSnapshot`

`SimpleTypeSerializerSnapshot` 适用于没有任何状态或配置的序列化器，本质上意味着序列化器的序列化模式仅由序列化器的类定义。

使用`SimpleTypeSerializerSnapshot`时，兼容性解析只有 2 种可能的结果作为序列化器的快照类：

- `TypeSerializerSchemaCompatibility.compatibleAsIs()`, 如果新的序列化器类保持相同，或者
- `TypeSerializerSchemaCompatibility.incompatible()`,如果新的序列化器类与前一个序列化器类不同。

下面以 Flink 的 IntSerializer 为例展示 SimpleTypeSerializerSnapshot 的使用方法：

```java
public class IntSerializerSnapshot extends SimpleTypeSerializerSnapshot<Integer> {
    public IntSerializerSnapshot() {
        super(() -> IntSerializer.INSTANCE);
    }
}
```

`IntSerializer` 没有状态或配置。 序列化格式仅由序列化器定义类本身，并且只能由另一个`IntSerializer`读取。 因此，它适合的用例
`SimpleTypeSerializerSnapshot`。

`SimpleTypeSerializerSnapshot`的基本超级构造函数需要实例的`Supplier`相应的序列化器，无论快照当前是正在恢复还是正在写入
快照。 该方法用于创建恢复序列化程序，以及类型检查以验证新的序列化器具有相同的预期序列化器类。

### 实现一个`CompositeTypeSerializerSnapshot`

`CompositeTypeSerializerSnapshot`适用于依赖多个嵌套序列化器进行序列化的序列化器。

在进一步解释之前，我们将依赖于多个嵌套序列化器的序列化器称为此上下文中的`外部`序列化器。示例可以是`MapSerializer`、`ListSerializer`、`GenericArraySerializer`等。例如，考虑`MapSerializer` 键和值序列化器将是嵌套序列化器，而`MapSerializer` 本身就是`外部`序列化器。

在这种情况下，外部序列化器的快照还应该包含嵌套序列化器的快照，以便可以独立检查嵌套序列化器的兼容性。 当解决兼容性问题时
外部序列化器，需要考虑每个嵌套序列化器的兼容性。

提供`CompositeTypeSerializerSnapshot`来帮助实现此类的快照复合序列化器。 它处理读取和写入嵌套序列化器快照，以及解析考虑所有嵌套序列化器的兼容性的最终兼容性结果。

下面以 Flink 的`MapSerializer` 为例介绍如何使用`CompositeTypeSerializerSnapshot` ：

```java
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
```

当将新的序列化器快照实现为`CompositeTypeSerializerSnapshot`的子类时，
必须实施以下三种方法：

* `#getCurrentOuterSnapshotVersion()`: 该方法定义了版本当前外部序列化器快照的序列化二进制格式。
* `#getNestedSerializers(TypeSerializer)`: 给定外部序列化器，返回其嵌套序列化器。
* `#createOuterSerializerWithNestedSerializers(TypeSerializer[])`:给定嵌套序列化器，创建外部序列化器的实例。

上面的示例是一个`CompositeTypeSerializerSnapshot`，其中没有需要快照的额外信息除了嵌套序列化器的快照之外。 因此，可以预期其外部快照版本永远不会需要上涨。 然而，其他一些序列化器包含一些额外的静态配置需要与嵌套组件序列化器一起保存。 Flink 就是一个例子`GenericArraySerializer`，其中包含数组元素类型的类作为配置，此外嵌套元素序列化器。

在这些情况下，需要在`CompositeTypeSerializerSnapshot`上实现另外三个方法：
* `#writeOuterSnapshot(DataOutputView)`: 定义了外部快照信息的写入方式。
* `#readOuterSnapshot(int, DataInputView, ClassLoader)`: 定义如何读取外部快照信息。
* `#resolveOuterSchemaCompatibility(TypeSerializer)`: 根据外部快照信息检查兼容性。

默认情况下，`CompositeTypeSerializerSnapshot`假设没有任何外部快照信息读/写，因此上述方法的默认实现为空。 如果子类有了外部快照信息，那么这三个方法都必须实现。

下面是如何将`CompositeTypeSerializerSnapshot`用于复合序列化器快照的示例，确实有外部快照信息，以 Flink 的 GenericArraySerializer 为例：

```java
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
```

上面的代码片段中有两件重要的事情需要注意。 首先，自从这`CompositeTypeSerializerSnapshot` 实现具有作为快照一部分写入的外部快照信息，外部快照版本（由 getCurrentOuterSnapshotVersion() 定义）必须在以下情况下升级：外层快照信息的序列化格式发生变化。

其次，请注意我们在编写组件类时如何避免使用 Java 序列化，只需编写类名并在读回快照时动态加载它。 避免 Java 序列化写入序列化器快照的内容通常是一个值得遵循的好习惯。 有关此内容的更多详细信息，请参见下一节。

## 实现说明和最佳实践

#### 1. Flink 通过使用类名实例化序列化器快照来恢复它们

序列化器的快照是注册状态如何序列化的唯一事实来源，可用作保存点中读取状态的入口点。 为了能够恢复和访问之前的状态，之前的状态序列化器的快照必须能够恢复。

Flink 通过首先使用其类名实例化`TypeSerializerSnapshot`（写为以及快照字节）。 因此，为了避免遭受意外的类名更改或实例化失败时，`TypeSerializerSnapshot`类应该：

- 避免被实现为匿名类或嵌套类，
- 有一个公共的、无效的构造函数用于实例化

#### 2. 避免在不同的序列化器之间共享相同的`TypeSerializerSnapshot`类

由于模式兼容性检查通过序列化器快照，因此多个序列化器返回与快照相同的`TypeSerializerSnapshot`类会使实现变得复杂
`TypeSerializerSnapshot#resolveSchemaCompatibility` 和`TypeSerializerSnapshot#restoreSerializer()` 方法。

这也是一个糟糕的关注点分离； 单个序列化器的序列化模式，配置以及如何恢复它，应该合并在它自己的专用`TypeSerializerSnapshot`类中。

#### 3. 避免对序列化器快照内容使用 Java 序列化

在写入持久序列化器快照的内容时，根本不应该使用 Java 序列化。举个例子，一个序列化器需要将其目标类型的类作为其快照的一部分进行持久化。类的信息应该通过写类名的方式来持久化，而不是直接序列化类使用Java。 读取快照时，会读取类名，并用于通过名称动态加载类。

这种做法确保序列化器快照始终可以安全读取。 在上面的例子中，如果类型类使用 Java 序列化进行持久化，一旦类实现发生更改，快照可能不再可读根据 Java 序列化细节，不再是二进制兼容的。

## 从 Flink 1.7 之前已弃用的序列化器快照 API 迁移

本节是从 Flink 1.7 之前存在的序列化器和序列化器快照进行 API 迁移的指南。

在 Flink 1.7 之前，序列化器快照被实现为`TypeSerializerConfigSnapshot`（现已弃用，将来最终将被删除，并被新的`TypeSerializerSnapshot`接口完全取代）。此外，序列化器模式兼容性检查的责任位于`TypeSerializer`中，
在`TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)`方法中实现。

新旧抽象之间的另一个主要区别是已弃用的`TypeSerializerConfigSnapshot`不具备实例化以前的序列化器的能力。 因此，如果你的序列化器仍然返回`TypeSerializerConfigSnapshot`的子类作为其快照，序列化器实例本身将始终使用 Java 序列化将其写入保存点，以便以前的序列化程序在恢复时可用。这是非常不可取的，因为恢复作业是否成功取决于可用性前一个序列化器的类，或者一般来说，序列化器实例是否可以在恢复时读回使用Java序列化的时间。 这意味着您只能使用与您的状态相同的序列化器，一旦您想要升级序列化器类或执行模式迁移，这可能会出现问题。

为了面向未来并能够灵活地迁移状态序列化器和模式，强烈建议从旧的抽象迁移。 执行此操作的步骤如下：

1. 实现`TypeSerializerSnapshot`的新子类。 这将是序列化器的新快照。
2. 返回新的`TypeSerializerSnapshot`作为序列化器的序列化器快照
   `TypeSerializer#snapshotConfiguration()` 方法。
3. 从 Flink 1.7 之前存在的保存点恢复作业，然后再次获取保存点。请注意，在这一步中，序列化器的旧`TypeSerializerConfigSnapshot`必须仍然存在于类路径中，并且`TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)` 方法的实现不能是已删除。 此过程的目的是替换旧保存点中写入的`TypeSerializerConfigSnapshot`使用序列化器新实现的`TypeSerializerSnapshot`。
4. 一旦您使用 Flink 1.7 获取了保存点，该保存点将包含`TypeSerializerSnapshot` 作为
   状态序列化器快照，并且序列化器实例将不再写入保存点中。
   此时，现在可以安全地删除旧抽象的所有实现（删除旧的抽象）
   `TypeSerializerConfigSnapshot` 实现将作为
   来自序列化器的`TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)`）。

{{< top >}}

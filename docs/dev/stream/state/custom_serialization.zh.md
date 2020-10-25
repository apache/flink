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

This page is targeted as a guideline for users who require the use of custom serialization for their state, covering
how to provide a custom state serializer as well as guidelines and best practices for implementing serializers that allow
state schema evolution.

If you're simply using Flink's own serializers, this page is irrelevant and can be ignored.

## Using custom state serializers

When registering a managed operator or keyed state, a `StateDescriptor` is required
to specify the state's name, as well as information about the type of the state. The type information is used by Flink's
[type serialization framework](../../types_serialization.html) to create appropriate serializers for the state.

It is also possible to completely bypass this and let Flink use your own custom serializer to serialize managed states,
simply by directly instantiating the `StateDescriptor` with your own `TypeSerializer` implementation:

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

This section explains the user-facing abstractions related to state serialization and schema evolution, and necessary
internal details about how Flink interacts with these abstractions.

When restoring from savepoints, Flink allows changing the serializers used to read and write previously registered state,
so that users are not locked in to any specific serialization schema. When state is restored, a new serializer will be
registered for the state (i.e., the serializer that comes with the `StateDescriptor` used to access the state in the
restored job). This new serializer may have a different schema than that of the previous serializer. Therefore, when
implementing state serializers, besides the basic logic of reading / writing data, another important thing to keep in
mind is how the serialization schema can be changed in the future.

When speaking of *schema*, in this context the term is interchangeable between referring to the *data model* of a state
type and the *serialized binary format* of a state type. The schema, generally speaking, can change for a few cases:

 1. Data schema of the state type has evolved, i.e. adding or removing a field from a POJO that is used as state.
 2. Generally speaking, after a change to the data schema, the serialization format of the serializer will need to be upgraded.
 3. Configuration of the serializer has changed.
 
In order for the new execution to have information about the *written schema* of state and detect whether or not the
schema has changed, upon taking a savepoint of an operator's state, a *snapshot* of the state serializer needs to be
written along with the state bytes. This is abstracted a `TypeSerializerSnapshot`, explained in the next subsection.

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

A serializer's `TypeSerializerSnapshot` is a point-in-time information that serves as the single source of truth about
the state serializer's write schema, as well as any additional information mandatory to restore a serializer that
would be identical to the given point-in-time. The logic about what should be written and read at restore time
as the serializer snapshot is defined in the `writeSnapshot` and `readSnapshot` methods.

Note that the snapshot's own write schema may also need to change over time (e.g. when you wish to add more information
about the serializer to the snapshot). To facilitate this, snapshots are versioned, with the current version
number defined in the `getCurrentVersion` method. On restore, when the serializer snapshot is read from savepoints,
the version of the schema in which the snapshot was written in will be provided to the `readSnapshot` method so that
the read implementation can handle different versions.

At restore time, the logic that detects whether or not the new serializer's schema has changed should be implemented in
the `resolveSchemaCompatibility` method. When previous registered state is registered again with new serializers in the
restored execution of an operator, the new serializer is provided to the previous serializer's snapshot via this method.
This method returns a `TypeSerializerSchemaCompatibility` representing the result of the compatibility resolution,
which can be one of the following:

 1. **`TypeSerializerSchemaCompatibility.compatibleAsIs()`**: this result signals that the new serializer is compatible,
 meaning that the new serializer has identical schema with the previous serializer. It is possible that the new
 serializer has been reconfigured in the `resolveSchemaCompatibility` method so that it is compatible.
 2. **`TypeSerializerSchemaCompatibility.compatibleAfterMigration()`**: this result signals that the new serializer has a
 different serialization schema, and it is possible to migrate from the old schema by using the previous serializer
 (which recognizes the old schema) to read bytes into state objects, and then rewriting the object back to bytes with
 the new serializer (which recognizes the new schema). 
 3. **`TypeSerializerSchemaCompatibility.incompatible()`**: this result signals that the new serializer has a
 different serialization schema, but it is not possible to migrate from the old schema.

The last bit of detail is how the previous serializer is obtained in the case that migration is required.
Another important role of a serializer's `TypeSerializerSnapshot` is that it serves as a factory to restore
the previous serializer. More specifically, the `TypeSerializerSnapshot` should implement the `restoreSerializer` method
to instantiate a serializer instance that recognizes the previous serializer's schema and configuration, and can therefore
safely read data written by the previous serializer.

### How Flink interacts with the `TypeSerializer` and `TypeSerializerSnapshot` abstractions

To wrap up, this section concludes how Flink, or more specifically the state backends, interact with the
abstractions. The interaction is slightly different depending on the state backend, but this is orthogonal
to the implementation of state serializers and their serializer snapshots.

#### Off-heap state backends (e.g. `RocksDBStateBackend`)

 1. **Register new state with a state serializer that has schema _A_**
  - the registered `TypeSerializer` for the state is used to read / write state on every state access.
  - State is written in schema *A*.
 2. **Take a savepoint**
  - The serializer snapshot is extracted via the `TypeSerializer#snapshotConfiguration` method.
  - The serializer snapshot is written to the savepoint, as well as the already-serialized state bytes (with schema *A*).
 3. **Restored execution re-accesses restored state bytes with new state serializer that has schema _B_**
  - The previous state serializer's snapshot is restored.
  - State bytes are not deserialized on restore, only loaded back to the state backends (therefore, still in schema *A*).
  - Upon receiving the new serializer, it is provided to the restored previous serializer's snapshot via the
  `TypeSerializer#resolveSchemaCompatibility` to check for schema compatibility.
 4. **Migrate state bytes in backend from schema _A_ to schema _B_**
  - If the compatibility resolution reflects that the schema has changed and migration is possible, schema migration is 
  performed. The previous state serializer which recognizes schema _A_ will be obtained from the serializer snapshot, via
   `TypeSerializerSnapshot#restoreSerializer()`, and is used to deserialize state bytes to objects, which in turn
   are re-written again with the new serializer, which recognizes schema _B_ to complete the migration. All entries
   of the accessed state is migrated all-together before processing continues.
  - If the resolution signals incompatibility, then the state access fails with an exception.
 
#### Heap state backends (e.g. `MemoryStateBackend`, `FsStateBackend`)

 1. **Register new state with a state serializer that has schema _A_**
  - the registered `TypeSerializer` is maintained by the state backend.
 2. **Take a savepoint, serializing all state with schema _A_**
  - The serializer snapshot is extracted via the `TypeSerializer#snapshotConfiguration` method.
  - The serializer snapshot is written to the savepoint.
  - State objects are now serialized to the savepoint, written in schema _A_.
 3. **On restore, deserialize state into objects in heap**
  - The previous state serializer's snapshot is restored.
  - The previous serializer, which recognizes schema _A_, is obtained from the serializer snapshot, via
  `TypeSerializerSnapshot#restoreSerializer()`, and is used to deserialize state bytes to objects.
  - From now on, all of the state is already deserialized.
 4. **Restored execution re-accesses previous state with new state serializer that has schema _B_**
  - Upon receiving the new serializer, it is provided to the restored previous serializer's snapshot via the
  `TypeSerializer#resolveSchemaCompatibility` to check for schema compatibility.
  - If the compatibility check signals that migration is required, nothing happens in this case since for
   heap backends, all state is already deserialized into objects.
  - If the resolution signals incompatibility, then the state access fails with an exception.
 5. **Take another savepoint, serializing all state with schema _B_**
  - Same as step 2., but now state bytes are all in schema _B_.

## Predefined convenient `TypeSerializerSnapshot` classes

Flink provides two abstract base `TypeSerializerSnapshot` classes that can be used for typical scenarios:
`SimpleTypeSerializerSnapshot` and `CompositeTypeSerializerSnapshot`.

Serializers that provide these predefined snapshots as their serializer snapshot must always have their own, independent
subclass implementation. This corresponds to the best practice of not sharing snapshot classes
across different serializers, which is more thoroughly explained in the next section.

### Implementing a `SimpleTypeSerializerSnapshot`

The `SimpleTypeSerializerSnapshot` is intended for serializers that do not have any state or configuration,
essentially meaning that the serialization schema of the serializer is solely defined by the serializer's class.

There will only be 2 possible results of the compatibility resolution when using the `SimpleTypeSerializerSnapshot`
as your serializer's snapshot class:

 - `TypeSerializerSchemaCompatibility.compatibleAsIs()`, if the new serializer class remains identical, or
 - `TypeSerializerSchemaCompatibility.incompatible()`, if the new serializer class is different then the previous one.
 
Below is an example of how the `SimpleTypeSerializerSnapshot` is used, using Flink's `IntSerializer` as an example:
<div data-lang="java" markdown="1">
{% highlight java %}
public class IntSerializerSnapshot extends SimpleTypeSerializerSnapshot<Integer> {
    public IntSerializerSnapshot() {
        super(() -> IntSerializer.INSTANCE);
    }
}
{% endhighlight %}
</div>

The `IntSerializer` has no state or configurations. Serialization format is solely defined by the serializer
class itself, and can only be read by another `IntSerializer`. Therefore, it suits the use case of the
`SimpleTypeSerializerSnapshot`.

The base super constructor of the `SimpleTypeSerializerSnapshot` expects a `Supplier` of instances
of the corresponding serializer, regardless of whether the snapshot is currently being restored or being written during
snapshots. That supplier is used to create the restore serializer, as well as type checks to verify that the
new serializer is of the same expected serializer class.

### Implementing a `CompositeTypeSerializerSnapshot`

The `CompositeTypeSerializerSnapshot` is intended for serializers that rely on multiple nested serializers for serialization.

Before further explanation, we call the serializer, which relies on multiple nested serializer(s), as the "outer" serializer in this context.
Examples for this could be `MapSerializer`, `ListSerializer`, `GenericArraySerializer`, etc.
Consider the `MapSerializer`, for example - the key and value serializers would be the nested serializers,
while `MapSerializer` itself is the "outer" serializer.

In this case, the snapshot of the outer serializer should also contain snapshots of the nested serializers, so that
the compatibility of the nested serializers can be independently checked. When resolving the compatibility of the
outer serializer, the compatibility of each nested serializer needs to be considered.

`CompositeTypeSerializerSnapshot` is provided to assist in the implementation of snapshots for these kind of
composite serializers. It deals with reading and writing the nested serializer snapshots, as well as resolving
the final compatibility result taking into account the compatibility of all nested serializers.

Below is an example of how the `CompositeTypeSerializerSnapshot` is used, using Flink's `MapSerializer` as an example:
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

When implementing a new serializer snapshot as a subclass of `CompositeTypeSerializerSnapshot`,
the following three methods must be implemented:
 * `#getCurrentOuterSnapshotVersion()`: This method defines the version of
   the current outer serializer snapshot's serialized binary format.
 * `#getNestedSerializers(TypeSerializer)`: Given the outer serializer, returns its nested serializers.
 * `#createOuterSerializerWithNestedSerializers(TypeSerializer[])`:
   Given the nested serializers, create an instance of the outer serializer.

The above example is a `CompositeTypeSerializerSnapshot` where there are no extra information to be snapshotted
apart from the nested serializers' snapshots. Therefore, its outer snapshot version can be expected to never
require an uptick. Some other serializers, however, contains some additional static configuration
that needs to be persisted along with the nested component serializer. An example for this would be Flink's
`GenericArraySerializer`, which contains as configuration the class of the array element type, besides
the nested element serializer.

In these cases, an additional three methods need to be implemented on the `CompositeTypeSerializerSnapshot`:
 * `#writeOuterSnapshot(DataOutputView)`: defines how the outer snapshot information is written.
 * `#readOuterSnapshot(int, DataInputView, ClassLoader)`: defines how the outer snapshot information is read.
 * `#resolveOuterSchemaCompatibility(TypeSerializer)`: checks the compatibility based on the outer snapshot information.

By default, the `CompositeTypeSerializerSnapshot` assumes that there isn't any outer snapshot information to
read / write, and therefore have empty default implementations for the above methods. If the subclass
has outer snapshot information, then all three methods must be implemented.

Below is an example of how the `CompositeTypeSerializerSnapshot` is used for composite serializer snapshots
that do have outer snapshot information, using Flink's `GenericArraySerializer` as an example:

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

There are two important things to notice in the above code snippet. First of all, since this
`CompositeTypeSerializerSnapshot` implementation has outer snapshot information that is written as part of the snapshot,
the outer snapshot version, as defined by `getCurrentOuterSnapshotVersion()`, must be upticked whenever the
serialization format of the outer snapshot information changes.

Second of all, notice how we avoid using Java serialization when writing the component class, by only writing
the classname and dynamically loading it when reading back the snapshot. Avoiding Java serialization for writing
contents of serializer snapshots is in general a good practice to follow. More details about this is covered in the
next section.

## Implementation notes and best practices

#### 1. Flink restores serializer snapshots by instantiating them with their classname

A serializer's snapshot, being the single source of truth for how a registered state was serialized, serves as an
entry point to reading state in savepoints. In order to be able to restore and access previous state, the previous state
serializer's snapshot must be able to be restored.

Flink restores serializer snapshots by first instantiating the `TypeSerializerSnapshot` with its classname (written
along with the snapshot bytes). Therefore, to avoid being subject to unintended classname changes or instantiation
failures, `TypeSerializerSnapshot` classes should:

 - avoid being implemented as anonymous classes or nested classes,
 - have a public, nullary constructor for instantiation

#### 2. Avoid sharing the same `TypeSerializerSnapshot` class across different serializers

Since schema compatibility checks goes through the serializer snapshots, having multiple serializers returning
the same `TypeSerializerSnapshot` class as their snapshot would complicate the implementation for the
`TypeSerializerSnapshot#resolveSchemaCompatibility` and `TypeSerializerSnapshot#restoreSerializer()` method.

This would also be a bad separation of concerns; a single serializer's serialization schema,
configuration, as well as how to restore it, should be consolidated in its own dedicated `TypeSerializerSnapshot` class.

#### 3. Avoid using Java serialization for serializer snapshot content

Java serialization should not be used at all when writing contents of a persisted serializer snapshot.
Take for example, a serializer which needs to persist a class of its target type as part of its snapshot.
Information about the class should be persisted by writing the class name, instead of directly serializing the class
using Java. When reading the snapshot, the class name is read, and used to dynamically load the class via the name.

This practice ensures that serializer snapshots can always be safely read. In the above example, if the type class
was persisted using Java serialization, the snapshot may no longer be readable once the class implementation has changed
and is no longer binary compatible according to Java serialization specifics.

## Migrating from deprecated serializer snapshot APIs before Flink 1.7

This section is a guide for API migration from serializers and serializer snapshots that existed before Flink 1.7.

Before Flink 1.7, serializer snapshots were implemented as a `TypeSerializerConfigSnapshot` (which is now deprecated,
and will eventually be removed in the future to be fully replaced by the new `TypeSerializerSnapshot` interface).
Moreover, the responsibility of serializer schema compatibility checks lived within the `TypeSerializer`,
implemented in the `TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)` method.

Another major difference between the new and old abstractions is that the deprecated `TypeSerializerConfigSnapshot`
did not have the capability of instantiating the previous serializer. Therefore, in the case where your serializer
still returns a subclass of `TypeSerializerConfigSnapshot` as its snapshot, the serializer instance itself will always
be written to savepoints using Java serialization so that the previous serializer may be available at restore time.
This is very undesirable, since whether or not restoring the job will be successful is susceptible to availability
of the previous serializer's class, or in general, whether or not the serializer instance can be read back at restore
time using Java serialization. This means that you be limited to the same serializer for your state,
and could be problematic once you want to upgrade serializer classes or perform schema migration.

To be future-proof and have flexibility to migrate your state serializers and schema, it is highly recommended to
migrate from the old abstractions. The steps to do this is as follows:

 1. Implement a new subclass of `TypeSerializerSnapshot`. This will be the new snapshot for your serializer.
 2. Return the new `TypeSerializerSnapshot` as the serializer snapshot for your serializer in the
 `TypeSerializer#snapshotConfiguration()` method.
 3. Restore the job from the savepoint that existed before Flink 1.7, and then take a savepoint again.
 Note that at this step, the old `TypeSerializerConfigSnapshot` of the serializer must still exist in the classpath,
 and the implementation for the `TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)` method must not be
 removed. The purpose of this process is to replace the `TypeSerializerConfigSnapshot` written in old savepoints
 with the newly implemented `TypeSerializerSnapshot` for the serializer.
 4. Once you have a savepoint taken with Flink 1.7, the savepoint will contain `TypeSerializerSnapshot` as the
 state serializer snapshot, and the serializer instance will no longer be written in the savepoint.
 At this point, it is now safe to remove all implementations of the old abstraction (remove the old
 `TypeSerializerConfigSnapshot` implementation as will as the
 `TypeSerializer#ensureCompatibility(TypeSerializerConfigSnapshot)` from the serializer).

{% top %}

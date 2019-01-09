---
title: "Custom Serialization for Managed State"
nav-title: "Custom State Serialization"
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

#### 3. Use the `CompositeSerializerSnapshot` utility for serializers that contain nested serializers

There may be cases where a `TypeSerializer` relies on other nested `TypeSerializer`s; take for example Flink's
`TupleSerializer`, where it is configured with nested `TypeSerializer`s for the tuple fields. In this case,
the snapshot of the most outer serializer should also contain snapshots of the nested serializers.

The `CompositeSerializerSnapshot` can be used specifically for this scenario. It wraps the logic of resolving
the overall schema compatibility check result for the composite serializer.
For an example of how it should be used, one can refer to Flink's
[ListSerializerSnapshot](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/typeutils/base/ListSerializerSnapshot.java) implementation.

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

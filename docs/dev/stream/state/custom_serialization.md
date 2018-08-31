---
title: "Custom Serialization for Managed State"
nav-title: "Custom Serialization"
nav-parent_id: streaming_state
nav-pos: 6
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

If your application uses Flink's managed state, it might be necessary to implement custom serialization logic for special use cases.

This page is targeted as a guideline for users who require the use of custom serialization for their state, covering how
to provide a custom serializer and how to handle upgrades to the serializer for compatibility. If you're simply using
Flink's own serializers, this page is irrelevant and can be skipped.

### Using custom serializers

As demonstrated in the above examples, when registering a managed operator or keyed state, a `StateDescriptor` is required
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

Note that Flink writes state serializers along with the state as metadata. In certain cases on restore (see following
subsections), the written serializer needs to be deserialized and used. Therefore, it is recommended to avoid using
anonymous classes as your state serializers. Anonymous classes do not have a guarantee on the generated classname,
which varies across compilers and depends on the order that they are instantiated within the enclosing class, which can 
easily cause the previously written serializer to be unreadable (since the original class can no longer be found in the
classpath).

### Handling serializer upgrades and compatibility

Flink allows changing the serializers used to read and write managed state, so that users are not locked in to any
specific serialization. When state is restored, the new serializer registered for the state (i.e., the serializer
that comes with the `StateDescriptor` used to access the state in the restored job) will be checked for compatibility,
and is replaced as the new serializer for the state.

A compatible serializer would mean that the serializer is capable of reading previous serialized bytes of the state,
and the written binary format of the state also remains identical. The means to check the new serializer's compatibility
is provided through the following two methods of the `TypeSerializer` interface:

{% highlight java %}
public abstract TypeSerializerConfigSnapshot snapshotConfiguration();
public abstract CompatibilityResult ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot);
{% endhighlight %}

Briefly speaking, every time a checkpoint is performed, the `snapshotConfiguration` method is called to create a
point-in-time view of the state serializer's configuration. The returned configuration snapshot is stored along with the
checkpoint as the state's metadata. When the checkpoint is used to restore a job, that serializer configuration snapshot
will be provided to the _new_ serializer of the same state via the counterpart method, `ensureCompatibility`, to verify
compatibility of the new serializer. This method serves as a check for whether or not the new serializer is compatible,
as well as a hook to possibly reconfigure the new serializer in the case that it is incompatible.

Note that Flink's own serializers are implemented such that they are at least compatible with themselves, i.e. when the
same serializer is used for the state in the restored job, the serializer's will reconfigure themselves to be compatible
with their previous configuration.

The following subsections illustrate guidelines to implement these two methods when using custom serializers.

#### Implementing the `snapshotConfiguration` method

The serializer's configuration snapshot should capture enough information such that on restore, the information
carried over to the new serializer for the state is sufficient for it to determine whether or not it is compatible.
This could typically contain information about the serializer's parameters or binary format of the serialized data;
generally, anything that allows the new serializer to decide whether or not it can be used to read previous serialized
bytes, and that it writes in the same binary format.

How the serializer's configuration snapshot is written to and read from checkpoints is fully customizable. The below
is the base class for all serializer configuration snapshot implementations, the `TypeSerializerConfigSnapshot`.

{% highlight java %}
public abstract TypeSerializerConfigSnapshot extends VersionedIOReadableWritable {
  public abstract int getVersion();
  public void read(DataInputView in) {...}
  public void write(DataOutputView out) {...}
}
{% endhighlight %}

The `read` and `write` methods define how the configuration is read from and written to the checkpoint. The base
implementations contain logic to read and write the version of the configuration snapshot, so it should be extended and
not completely overridden.

The version of the configuration snapshot is determined through the `getVersion` method. Versioning for the serializer
configuration snapshot is the means to maintain compatible configurations, as information included in the configuration
may change over time. By default, configuration snapshots are only compatible with the current version (as returned by
`getVersion`). To indicate that the configuration is compatible with other versions, override the `getCompatibleVersions`
method to return more version values. When reading from the checkpoint, you can use the `getReadVersion` method to
determine the version of the written configuration and adapt the read logic to the specific version.

<span class="label label-danger">Attention</span> The version of the serializer's configuration snapshot is **not**
related to upgrading the serializer. The exact same serializer can have different implementations of its
configuration snapshot, for example when more information is added to the configuration to allow more comprehensive
compatibility checks in the future.

One limitation of implementing a `TypeSerializerConfigSnapshot` is that an empty constructor must be present. The empty
constructor is required when reading the configuration snapshot from checkpoints.

#### Implementing the `ensureCompatibility` method

The `ensureCompatibility` method should contain logic that performs checks against the information about the previous
serializer carried over via the provided `TypeSerializerConfigSnapshot`, basically doing one of the following:

  * Check whether the serializer is compatible, while possibly reconfiguring itself (if required) so that it may be
    compatible. Afterwards, acknowledge with Flink that the serializer is compatible.

  * Acknowledge that the serializer is incompatible and that state migration is required before Flink can proceed with
    using the new serializer.

The above cases can be translated to code by returning one of the following from the `ensureCompatibility` method:

  * **`CompatibilityResult.compatible()`**: This acknowledges that the new serializer is compatible, or has been reconfigured to
    be compatible, and Flink can proceed with the job with the serializer as is.

  * **`CompatibilityResult.requiresMigration()`**: This acknowledges that the serializer is incompatible, or cannot be
    reconfigured to be compatible, and requires a state migration before the new serializer can be used. State migration
    is performed by using the previous serializer to read the restored state bytes to objects, and then serialized again
    using the new serializer.

  * **`CompatibilityResult.requiresMigration(TypeDeserializer deserializer)`**: This acknowledgement has equivalent semantics
    to `CompatibilityResult.requiresMigration()`, but in the case that the previous serializer cannot be found or loaded
    to read the restored state bytes for the migration, a provided `TypeDeserializer` can be used as a fallback resort.

<span class="label label-danger">Attention</span> Currently, as of Flink 1.3, if the result of the compatibility check
acknowledges that state migration needs to be performed, the job simply fails to restore from the checkpoint as state
migration is currently not available. The ability to migrate state will be introduced in future releases.

### Managing `TypeSerializer` and `TypeSerializerConfigSnapshot` classes in user code

Since `TypeSerializer`s and `TypeSerializerConfigSnapshot`s are written as part of checkpoints along with the state
values, the availability of the classes within the classpath may affect restore behaviour.

`TypeSerializer`s are directly written into checkpoints using Java Object Serialization. In the case that the new
serializer acknowledges that it is incompatible and requires state migration, it will be required to be present to be
able to read the restored state bytes. Therefore, if the original serializer class no longer exists or has been modified
(resulting in a different `serialVersionUID`) as a result of a serializer upgrade for the state, the restore would
not be able to proceed. The alternative to this requirement is to provide a fallback `TypeDeserializer` when
acknowledging that state migration is required, using `CompatibilityResult.requiresMigration(TypeDeserializer deserializer)`.

The class of `TypeSerializerConfigSnapshot`s in the restored checkpoint must exist in the classpath, as they are
fundamental components to compatibility checks on upgraded serializers and would not be able to be restored if the class
is not present. Since configuration snapshots are written to checkpoints using custom serialization, the implementation
of the class is free to be changed, as long as compatibility of the configuration change is handled using the versioning
mechanisms in `TypeSerializerConfigSnapshot`.

{% top %}

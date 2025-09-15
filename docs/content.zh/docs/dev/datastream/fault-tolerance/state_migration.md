---
title: "State TTL Migration Compatibility"
weight: 1
type: docs
aliases:
  - /dev/stream/state/state_migration.html
  - /apis/streaming/state_migration.html
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

# State TTL Migration Compatibility

Starting with **Apache Flink 2.2.0**, the system supports seamless enabling or disabling of **State Time-to-Live (TTL)** for existing state.
This enhancement removes prior limitations where a change in TTL configuration could cause a `StateMigrationException` during restore.

## Version Overview

| Flink Version | Change                                                                          |
| ------------- | ------------------------------------------------------------------------------- |
| **2.0.0**     | Introduced `TtlAwareSerializer` to support TTL/non-TTL serializer compatibility |
| **2.1.0**     | Added TTL migration support for **RocksDBKeyedStateBackend**                    |
| **2.2.0**     | Added TTL migration support for **HeapKeyedStateBackend**                       |

> Full TTL state migration support across all major state backends is available from Flink **2.2.0** onwards.

## Motivation

In earlier Flink versions, switching TTL on or off in a `StateDescriptor` resulted in incompatibility errors.
This was because TTL-enabled state used a different serialization format than non-TTL state.

## Compatibility Behavior

With the changes introduced across versions 2.0.0 to 2.2.0:

* Flink can now restore state created **without TTL** using a descriptor **with TTL enabled**.
* Flink can also restore state created **with TTL** using a descriptor **without TTL enabled**.

The serializers and state backends transparently handle the presence or absence of TTL metadata.

## Supported Migration Scenarios

| Migration Type                         | Available Since               | Behavior                                                            |
| -------------------------------------- | ----------------------------- | ------------------------------------------------------------------- |
| Non-TTL state → TTL-enabled descriptor | 2.1.0 (RocksDB), 2.2.0 (Heap) | Previous state restored as non-expired. TTL applied on new updates/accesses. |
| TTL state → Non-TTL descriptor         | 2.1.0 (RocksDB), 2.2.0 (Heap) | TTL metadata is ignored. State becomes permanently visible.         |

## Implementation Details

The compatibility is achieved through the following changes:

* **TtlAwareSerializer** (Flink 2.0.0): Wraps user serializers to support reading/writing TTL and non-TTL formats.
* **Backend migration logic**:
    * RocksDBKeyedStateBackend (Flink 2.1.0)
    * HeapKeyedStateBackend (Flink 2.2.0)

These components check the metadata during restore and adapt accordingly.

## Limitations

* Changes to TTL **parameters** (e.g. expiration time, update behavior) are not always compatible. These may require serializer migration.
* TTL is not applied retroactively. Existing entries restored from non-TTL state will only expire after their next access or update.
* This compatibility assumes no other incompatible changes to the state serializer.

## Example

```java
ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("user-state", String.class);
descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1)).build());
```

If this descriptor replaces an earlier one without TTL, the state will be restored successfully in Flink 2.2.0+ and TTL will be enforced going forward.

## Related Information

* [Working with State]({{< ref "docs/dev/datastream/fault-tolerance/state" >}})
* [FLINK-32955 JIRA](https://issues.apache.org/jira/browse/FLINK-32955)

## FAQ

### Can I disable TTL after it was previously enabled?

Yes. Flink will restore the values and ignore any TTL expiration metadata.

### Is this supported in RocksDB and Heap backends?

Yes, RocksDB since 2.1.0, Heap since 2.2.0.

### Which Flink version fully supports TTL migration?

Flink 2.2.0 is the first version where all necessary support is available.

### Do I need to change anything in my savepoint?

No. The migration is handled internally by Flink, provided serializers are otherwise compatible.

{{< top >}}

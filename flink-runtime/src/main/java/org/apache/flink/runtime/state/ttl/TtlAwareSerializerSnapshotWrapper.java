/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wrap the TypeSerializerSnapshot restored from {@link StateMetaInfoSnapshot} to
 * TtlAwareSerializerSnapshot
 */
public class TtlAwareSerializerSnapshotWrapper<T> {

    private final StateDescriptor.Type stateType;
    private final TypeSerializerSnapshot<T> typeSerializerSnapshot;

    private final Map<StateDescriptor.Type, Supplier<TypeSerializerSnapshot<T>>>
            ttlAwareSerializerSnapshotFactories;

    @SuppressWarnings("unchecked")
    public TtlAwareSerializerSnapshotWrapper(@Nonnull StateMetaInfoSnapshot snapshot) {
        this.stateType =
                StateDescriptor.Type.valueOf(
                        snapshot.getOption(
                                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE));
        this.typeSerializerSnapshot =
                (TypeSerializerSnapshot<T>)
                        Preconditions.checkNotNull(
                                snapshot.getTypeSerializerSnapshot(
                                        StateMetaInfoSnapshot.CommonSerializerKeys
                                                .VALUE_SERIALIZER));
        this.ttlAwareSerializerSnapshotFactories = createTtlAwareSerializerSnapshotFactories();
    }

    @VisibleForTesting
    public TtlAwareSerializerSnapshotWrapper(
            StateDescriptor.Type stateType, TypeSerializerSnapshot<T> typeSerializerSnapshot) {
        this.stateType = stateType;
        this.typeSerializerSnapshot = typeSerializerSnapshot;
        this.ttlAwareSerializerSnapshotFactories = createTtlAwareSerializerSnapshotFactories();
    }

    private Map<StateDescriptor.Type, Supplier<TypeSerializerSnapshot<T>>>
            createTtlAwareSerializerSnapshotFactories() {
        return Stream.of(
                        Tuple2.of(
                                StateDescriptor.Type.VALUE,
                                (Supplier<TypeSerializerSnapshot<T>>)
                                        this::wrapValueSerializerSnapshot),
                        Tuple2.of(
                                StateDescriptor.Type.LIST,
                                (Supplier<TypeSerializerSnapshot<T>>)
                                        this::wrapListSerializerSnapshot),
                        Tuple2.of(
                                StateDescriptor.Type.MAP,
                                (Supplier<TypeSerializerSnapshot<T>>)
                                        this::wrapMapSerializerSnapshot),
                        Tuple2.of(
                                StateDescriptor.Type.REDUCING,
                                (Supplier<TypeSerializerSnapshot<T>>)
                                        this::wrapValueSerializerSnapshot),
                        Tuple2.of(
                                StateDescriptor.Type.AGGREGATING,
                                (Supplier<TypeSerializerSnapshot<T>>)
                                        this::wrapValueSerializerSnapshot))
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    public TypeSerializerSnapshot<T> getTtlAwareSerializerSnapshot() {
        return ttlAwareSerializerSnapshotFactories.get(stateType).get();
    }

    private TypeSerializerSnapshot<T> wrapValueSerializerSnapshot() {
        return typeSerializerSnapshot instanceof TtlAwareSerializerSnapshot
                ? typeSerializerSnapshot
                : new TtlAwareSerializerSnapshot<>(typeSerializerSnapshot);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TypeSerializerSnapshot<T> wrapListSerializerSnapshot() {
        ListSerializerSnapshot listSerializerSnapshot =
                (ListSerializerSnapshot) typeSerializerSnapshot;
        if (!(listSerializerSnapshot.getElementSerializerSnapshot()
                instanceof TtlAwareSerializerSnapshot)) {
            CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
                    listSerializerSnapshot,
                    new TtlAwareSerializerSnapshot<>(
                            listSerializerSnapshot.getElementSerializerSnapshot()));
        }
        return listSerializerSnapshot;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TypeSerializerSnapshot<T> wrapMapSerializerSnapshot() {
        MapSerializerSnapshot mapSerializerSnapshot =
                (MapSerializerSnapshot) typeSerializerSnapshot;
        if (!(mapSerializerSnapshot.getValueSerializerSnapshot()
                instanceof TtlAwareSerializerSnapshot)) {
            CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
                    mapSerializerSnapshot,
                    mapSerializerSnapshot.getKeySerializerSnapshot(),
                    new TtlAwareSerializerSnapshot<>(
                            mapSerializerSnapshot.getValueSerializerSnapshot()));
        }
        return mapSerializerSnapshot;
    }
}

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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TtlAwareSerializerSnapshotFactory<T> {

    private final StateDescriptor.Type stateType;
    private final TypeSerializerSnapshot<T> typeSerializerSnapshot;

    private final Map<StateDescriptor.Type, Supplier<TypeSerializerSnapshot<T>>>
            wrapTypeSerializerSnapshotWithTtlAwareFactories;

    public TtlAwareSerializerSnapshotFactory(
            StateDescriptor.Type stateType, TypeSerializerSnapshot<T> typeSerializerSnapshot) {
        this.stateType = stateType;
        this.typeSerializerSnapshot = typeSerializerSnapshot;
        this.wrapTypeSerializerSnapshotWithTtlAwareFactories =
                createWrapStateDescWithTtlAwareFactories();
    }

    private Map<StateDescriptor.Type, Supplier<TypeSerializerSnapshot<T>>>
            createWrapStateDescWithTtlAwareFactories() {
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

    public TypeSerializerSnapshot<T> getWrappedTtlAwareTypeSerializerSnapshot() {
        return wrapTypeSerializerSnapshotWithTtlAwareFactories.get(stateType).get();
    }

    private TypeSerializerSnapshot<T> wrapValueSerializerSnapshot() {
        return typeSerializerSnapshot instanceof TtlAwareSerializerSnapshot
                ? typeSerializerSnapshot
                : new TtlAwareSerializerSnapshotFactory.TtlAwareSerializerSnapshot<>(
                        typeSerializerSnapshot);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private TypeSerializerSnapshot<T> wrapListSerializerSnapshot() {
        ListSerializerSnapshot listSerializerSnapshot =
                (ListSerializerSnapshot) typeSerializerSnapshot;
        if (!(listSerializerSnapshot.getElementSerializerSnapshot()
                instanceof TtlAwareSerializerSnapshot)) {
            CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
                    listSerializerSnapshot,
                    new TtlAwareSerializerSnapshotFactory.TtlAwareSerializerSnapshot<>(
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
                    new TtlAwareSerializerSnapshotFactory.TtlAwareSerializerSnapshot<>(
                            mapSerializerSnapshot.getValueSerializerSnapshot()));
        }
        return mapSerializerSnapshot;
    }

    public static class TtlAwareSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

        private static final int CURRENT_VERSION = 1;

        private boolean isTtlEnabled;

        private TypeSerializerSnapshot<T> typeSerializerSnapshot;

        @SuppressWarnings({"WeakerAccess", "unused"})
        public TtlAwareSerializerSnapshot() {}

        public TtlAwareSerializerSnapshot(
                TypeSerializerSnapshot<T> typeSerializerSnapshot, boolean isTtlEnabled) {
            this.typeSerializerSnapshot = typeSerializerSnapshot;
            this.isTtlEnabled = isTtlEnabled;
        }

        public TtlAwareSerializerSnapshot(TypeSerializerSnapshot<T> typeSerializerSnapshot) {
            this.typeSerializerSnapshot = typeSerializerSnapshot;
            this.isTtlEnabled =
                    typeSerializerSnapshot instanceof TtlStateFactory.TtlSerializerSnapshot;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeBoolean(isTtlEnabled);
            TypeSerializerSnapshot.writeVersionedSnapshot(out, typeSerializerSnapshot);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            this.isTtlEnabled = in.readBoolean();
            this.typeSerializerSnapshot =
                    TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader);
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            return new TtlAwareSerializer<>(typeSerializerSnapshot.restoreSerializer());
        }

        @Override
        @SuppressWarnings("unchecked")
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializerSnapshot<T> oldSerializerSnapshot) {
            if (!(oldSerializerSnapshot instanceof TtlAwareSerializerSnapshot)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            TtlAwareSerializerSnapshot<T> oldTtlAwareSerializerSnapshot =
                    (TtlAwareSerializerSnapshot<T>) oldSerializerSnapshot;
            if (!oldTtlAwareSerializerSnapshot.isTtlEnabled() && this.isTtlEnabled()) {
                // Check compatibility from disabling to enabling ttl
                TtlStateFactory.TtlSerializerSnapshot<T> newSerializerSnapshot =
                        (TtlStateFactory.TtlSerializerSnapshot<T>)
                                this.getOrinalTypeSerializerSnapshot();
                TypeSerializerSchemaCompatibility<T> compatibility =
                        newSerializerSnapshot
                                .getValueSerializerSnapshot()
                                .resolveSchemaCompatibility(
                                        oldTtlAwareSerializerSnapshot
                                                .getOrinalTypeSerializerSnapshot());
                return TypeSerializerSchemaCompatibility.resolveCompatibilityForTtlMigration(
                        compatibility);
            } else if (oldTtlAwareSerializerSnapshot.isTtlEnabled() && !this.isTtlEnabled()) {
                // Check compatibility from enabling to disabling ttl
                TtlStateFactory.TtlSerializerSnapshot<T> oldTtlSerializerSnapshot =
                        (TtlStateFactory.TtlSerializerSnapshot<T>)
                                oldTtlAwareSerializerSnapshot.getOrinalTypeSerializerSnapshot();
                TypeSerializerSchemaCompatibility<T> compatibility =
                        this.getOrinalTypeSerializerSnapshot()
                                .resolveSchemaCompatibility(
                                        oldTtlSerializerSnapshot.getValueSerializerSnapshot());
                return TypeSerializerSchemaCompatibility.resolveCompatibilityForTtlMigration(
                        compatibility);
            }

            // Check compatibility with the same ttl config
            return this.getOrinalTypeSerializerSnapshot()
                    .resolveSchemaCompatibility(
                            oldTtlAwareSerializerSnapshot.getOrinalTypeSerializerSnapshot());
        }

        public boolean isTtlEnabled() {
            return isTtlEnabled;
        }

        public TypeSerializerSnapshot<T> getOrinalTypeSerializerSnapshot() {
            return typeSerializerSnapshot;
        }
    }
}

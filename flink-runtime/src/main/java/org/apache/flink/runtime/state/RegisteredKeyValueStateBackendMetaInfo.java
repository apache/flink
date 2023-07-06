/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Compound meta information for a registered state in a keyed state backend. This combines all
 * serializers and the state name.
 *
 * @param <N> Type of namespace
 * @param <S> Type of state value
 */
public class RegisteredKeyValueStateBackendMetaInfo<N, S> extends RegisteredStateMetaInfoBase {

    @Nonnull private final StateDescriptor.Type stateType;
    @Nonnull private final StateSerializerProvider<N> namespaceSerializerProvider;
    @Nonnull private final StateSerializerProvider<S> stateSerializerProvider;
    @Nonnull private StateSnapshotTransformFactory<S> stateSnapshotTransformFactory;

    public RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull String name,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull TypeSerializer<S> stateSerializer) {

        this(
                stateType,
                name,
                StateSerializerProvider.fromNewRegisteredSerializer(namespaceSerializer),
                StateSerializerProvider.fromNewRegisteredSerializer(stateSerializer),
                StateSnapshotTransformFactory.noTransform());
    }

    public RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull String name,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull TypeSerializer<S> stateSerializer,
            @Nonnull StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {

        this(
                stateType,
                name,
                StateSerializerProvider.fromNewRegisteredSerializer(namespaceSerializer),
                StateSerializerProvider.fromNewRegisteredSerializer(stateSerializer),
                stateSnapshotTransformFactory);
    }

    @SuppressWarnings("unchecked")
    public RegisteredKeyValueStateBackendMetaInfo(@Nonnull StateMetaInfoSnapshot snapshot) {
        this(
                StateDescriptor.Type.valueOf(
                        snapshot.getOption(
                                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE)),
                snapshot.getName(),
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        (TypeSerializerSnapshot<N>)
                                Preconditions.checkNotNull(
                                        snapshot.getTypeSerializerSnapshot(
                                                StateMetaInfoSnapshot.CommonSerializerKeys
                                                        .NAMESPACE_SERIALIZER))),
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        (TypeSerializerSnapshot<S>)
                                Preconditions.checkNotNull(
                                        snapshot.getTypeSerializerSnapshot(
                                                StateMetaInfoSnapshot.CommonSerializerKeys
                                                        .VALUE_SERIALIZER))),
                StateSnapshotTransformFactory.noTransform());

        Preconditions.checkState(
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE == snapshot.getBackendStateType());
    }

    private RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull String name,
            @Nonnull StateSerializerProvider<N> namespaceSerializerProvider,
            @Nonnull StateSerializerProvider<S> stateSerializerProvider,
            @Nonnull StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {

        super(name);
        this.stateType = stateType;
        this.namespaceSerializerProvider = namespaceSerializerProvider;
        this.stateSerializerProvider = stateSerializerProvider;
        this.stateSnapshotTransformFactory = stateSnapshotTransformFactory;
    }

    @Nonnull
    public StateDescriptor.Type getStateType() {
        return stateType;
    }

    @Nonnull
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializerProvider.currentSchemaSerializer();
    }

    @Nonnull
    public TypeSerializerSchemaCompatibility<N> updateNamespaceSerializer(
            TypeSerializer<N> newNamespaceSerializer) {
        return namespaceSerializerProvider.registerNewSerializerForRestoredState(
                newNamespaceSerializer);
    }

    @Nonnull
    public TypeSerializer<N> getPreviousNamespaceSerializer() {
        return namespaceSerializerProvider.previousSchemaSerializer();
    }

    @Nonnull
    public TypeSerializer<S> getStateSerializer() {
        return stateSerializerProvider.currentSchemaSerializer();
    }

    @Nonnull
    public TypeSerializerSchemaCompatibility<S> updateStateSerializer(
            TypeSerializer<S> newStateSerializer) {
        return stateSerializerProvider.registerNewSerializerForRestoredState(newStateSerializer);
    }

    @Nonnull
    public TypeSerializer<S> getPreviousStateSerializer() {
        return stateSerializerProvider.previousSchemaSerializer();
    }

    @Nullable
    public TypeSerializerSnapshot<S> getPreviousStateSerializerSnapshot() {
        return stateSerializerProvider.previousSerializerSnapshot;
    }

    @Nonnull
    public StateSnapshotTransformFactory<S> getStateSnapshotTransformFactory() {
        return stateSnapshotTransformFactory;
    }

    public void updateSnapshotTransformFactory(
            StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {
        this.stateSnapshotTransformFactory = stateSnapshotTransformFactory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegisteredKeyValueStateBackendMetaInfo<?, ?> that =
                (RegisteredKeyValueStateBackendMetaInfo<?, ?>) o;

        if (!stateType.equals(that.stateType)) {
            return false;
        }

        if (!getName().equals(that.getName())) {
            return false;
        }

        return getStateSerializer().equals(that.getStateSerializer())
                && getNamespaceSerializer().equals(that.getNamespaceSerializer());
    }

    @Override
    public String toString() {
        return "RegisteredKeyedBackendStateMetaInfo{"
                + "stateType="
                + stateType
                + ", name='"
                + name
                + '\''
                + ", namespaceSerializer="
                + getNamespaceSerializer()
                + ", stateSerializer="
                + getStateSerializer()
                + '}';
    }

    @Override
    public int hashCode() {
        int result = getName().hashCode();
        result = 31 * result + getStateType().hashCode();
        result = 31 * result + getNamespaceSerializer().hashCode();
        result = 31 * result + getStateSerializer().hashCode();
        return result;
    }

    @Nonnull
    @Override
    public StateMetaInfoSnapshot snapshot() {
        return computeSnapshot();
    }

    @Nonnull
    @Override
    public RegisteredKeyValueStateBackendMetaInfo<N, S> withSerializerUpgradesAllowed() {
        return new RegisteredKeyValueStateBackendMetaInfo<>(snapshot());
    }

    public void checkStateMetaInfo(StateDescriptor<?, ?> stateDesc) {

        Preconditions.checkState(
                Objects.equals(stateDesc.getName(), getName()),
                "Incompatible state names. "
                        + "Was ["
                        + getName()
                        + "], "
                        + "registered with ["
                        + stateDesc.getName()
                        + "].");

        if (stateDesc.getType() != StateDescriptor.Type.UNKNOWN
                && getStateType() != StateDescriptor.Type.UNKNOWN) {
            Preconditions.checkState(
                    stateDesc.getType() == getStateType(),
                    "Incompatible key/value state types. "
                            + "Was ["
                            + getStateType()
                            + "], "
                            + "registered with ["
                            + stateDesc.getType()
                            + "].");
        }
    }

    @Nonnull
    private StateMetaInfoSnapshot computeSnapshot() {
        Map<String, String> optionsMap =
                Collections.singletonMap(
                        StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
                        stateType.toString());
        Map<String, TypeSerializer<?>> serializerMap = CollectionUtil.newHashMapWithExpectedSize(2);
        Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotsMap =
                CollectionUtil.newHashMapWithExpectedSize(2);
        String namespaceSerializerKey =
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString();
        String valueSerializerKey =
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString();

        TypeSerializer<N> namespaceSerializer = getNamespaceSerializer();
        serializerMap.put(namespaceSerializerKey, namespaceSerializer.duplicate());
        serializerConfigSnapshotsMap.put(
                namespaceSerializerKey, namespaceSerializer.snapshotConfiguration());

        TypeSerializer<S> stateSerializer = getStateSerializer();
        serializerMap.put(valueSerializerKey, stateSerializer.duplicate());
        serializerConfigSnapshotsMap.put(
                valueSerializerKey, stateSerializer.snapshotConfiguration());

        return new StateMetaInfoSnapshot(
                name,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                optionsMap,
                serializerConfigSnapshotsMap,
                serializerMap);
    }
}

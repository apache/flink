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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.Map;

/**
 * Compound meta information for a registered state in a keyed state backend. This combines all
 * serializers and the state name.
 *
 * @param <S> Type of state value
 */
public class RegisteredKeyValueStateBackendMetaInfo<S> extends RegisteredStateMetaInfoBase {

    @Nonnull private final StateDescriptor.Type stateType;
    @Nonnull private final StateSerializerProvider<S> stateSerializerProvider;
    @Nonnull private StateSnapshotTransformFactory<S> stateSnapshotTransformFactory;

    public RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull String name,
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull TypeSerializer<S> stateSerializer) {

        this(
                name,
                stateType,
                StateSerializerProvider.fromNewRegisteredSerializer(stateSerializer),
                StateSnapshotTransformFactory.noTransform());
    }

    public RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull String name,
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull TypeSerializer<S> stateSerializer,
            @Nonnull StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {

        this(
                name,
                stateType,
                StateSerializerProvider.fromNewRegisteredSerializer(stateSerializer),
                stateSnapshotTransformFactory);
    }

    @SuppressWarnings("unchecked")
    public RegisteredKeyValueStateBackendMetaInfo(@Nonnull StateMetaInfoSnapshot snapshot) {
        this(
                snapshot.getName(),
                StateDescriptor.Type.valueOf(
                        snapshot.getOption(
                                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE)),
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        (TypeSerializerSnapshot<S>)
                                Preconditions.checkNotNull(
                                        snapshot.getTypeSerializerSnapshot(
                                                StateMetaInfoSnapshot.CommonSerializerKeys
                                                        .VALUE_SERIALIZER))),
                StateSnapshotTransformFactory.noTransform());

        Preconditions.checkState(
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2
                        == snapshot.getBackendStateType());
    }

    private RegisteredKeyValueStateBackendMetaInfo(
            @Nonnull String name,
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull StateSerializerProvider<S> stateSerializerProvider,
            @Nonnull StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {

        super(name);
        this.stateType = stateType;
        this.stateSerializerProvider = stateSerializerProvider;
        this.stateSnapshotTransformFactory = stateSnapshotTransformFactory;
    }

    @Nonnull
    public StateDescriptor.Type getStateType() {
        return stateType;
    }

    @Nonnull
    public TypeSerializer<S> getStateSerializer() {
        return stateSerializerProvider.currentSchemaSerializer();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegisteredKeyValueStateBackendMetaInfo<?> that =
                (RegisteredKeyValueStateBackendMetaInfo<?>) o;

        if (!stateType.equals(that.stateType)) {
            return false;
        }

        if (!getName().equals(that.getName())) {
            return false;
        }

        return getStateSerializer().equals(that.getStateSerializer());
    }

    @Override
    public String toString() {
        return "RegisteredKeyedBackendStateMetaInfo{"
                + "stateType="
                + stateType
                + ", name='"
                + name
                + '\''
                + ", stateSerializer="
                + getStateSerializer()
                + '}';
    }

    @Override
    public int hashCode() {
        int result = getName().hashCode();
        result = 31 * result + getStateType().hashCode();
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
    public RegisteredKeyValueStateBackendMetaInfo<S> withSerializerUpgradesAllowed() {
        return new RegisteredKeyValueStateBackendMetaInfo<>(snapshot());
    }

    @Nonnull
    private StateMetaInfoSnapshot computeSnapshot() {
        Map<String, String> optionsMap =
                Collections.singletonMap(
                        StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
                        stateType.toString());

        Map<String, TypeSerializer<?>> serializerMap = CollectionUtil.newHashMapWithExpectedSize(1);
        Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotsMap =
                CollectionUtil.newHashMapWithExpectedSize(1);

        String valueSerializerKey =
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString();
        TypeSerializer<S> stateSerializer = getStateSerializer();

        serializerMap.put(valueSerializerKey, stateSerializer.duplicate());
        serializerConfigSnapshotsMap.put(
                valueSerializerKey, stateSerializer.snapshotConfiguration());

        return new StateMetaInfoSnapshot(
                name,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2,
                optionsMap,
                serializerConfigSnapshotsMap,
                serializerMap);
    }
}

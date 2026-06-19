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

import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.StateSerializerProvider;
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
 * @param <S> Type of state value
 */
public class RegisteredKeyAndUserKeyValueStateBackendMetaInfo<N, UK, S>
        extends RegisteredKeyValueStateBackendMetaInfo<N, S> {

    // We keep it @Nullable since in the very first version we did not store this serializer here.
    @Nullable private StateSerializerProvider<UK> userKeySerializerProvider;

    public RegisteredKeyAndUserKeyValueStateBackendMetaInfo(
            @Nonnull String name,
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull TypeSerializer<S> stateSerializer,
            @Nullable TypeSerializer<UK> userKeySerializer) {

        this(
                name,
                stateType,
                StateSerializerProvider.fromNewRegisteredSerializer(namespaceSerializer),
                StateSerializerProvider.fromNewRegisteredSerializer(stateSerializer),
                userKeySerializer == null
                        ? null
                        : StateSerializerProvider.fromNewRegisteredSerializer(userKeySerializer),
                StateSnapshotTransformFactory.noTransform());
    }

    @SuppressWarnings("unchecked")
    public RegisteredKeyAndUserKeyValueStateBackendMetaInfo(
            @Nonnull StateMetaInfoSnapshot snapshot) {
        this(
                snapshot.getName(),
                StateDescriptor.Type.valueOf(
                        snapshot.getOption(
                                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE)),
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
                snapshot.getTypeSerializerSnapshot(
                                        StateMetaInfoSnapshot.CommonSerializerKeys
                                                .USER_KEY_SERIALIZER)
                                == null
                        ? null
                        : StateSerializerProvider.fromPreviousSerializerSnapshot(
                                (TypeSerializerSnapshot<UK>)
                                        Preconditions.checkNotNull(
                                                snapshot.getTypeSerializerSnapshot(
                                                        StateMetaInfoSnapshot.CommonSerializerKeys
                                                                .USER_KEY_SERIALIZER))),
                StateSnapshotTransformFactory.noTransform());

        Preconditions.checkState(
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2
                        == snapshot.getBackendStateType());
    }

    public RegisteredKeyAndUserKeyValueStateBackendMetaInfo(
            @Nonnull String name,
            @Nonnull StateDescriptor.Type stateType,
            @Nonnull StateSerializerProvider<N> namespaceSerializerProvider,
            @Nonnull StateSerializerProvider<S> stateSerializerProvider,
            @Nullable StateSerializerProvider<UK> userKeySerializerProvider,
            @Nonnull StateSnapshotTransformFactory<S> stateSnapshotTransformFactory) {
        super(
                name,
                stateType,
                namespaceSerializerProvider,
                stateSerializerProvider,
                stateSnapshotTransformFactory);
        this.userKeySerializerProvider = userKeySerializerProvider;
    }

    @Nullable
    public TypeSerializer<UK> getUserKeySerializer() {
        return userKeySerializerProvider == null
                ? null
                : userKeySerializerProvider.currentSchemaSerializer();
    }

    @Nonnull
    public TypeSerializerSchemaCompatibility<UK> updateUserKeySerializer(
            TypeSerializer<UK> newStateSerializer) {
        if (userKeySerializerProvider == null) {
            // This means that there is no userKeySerializerProvider in the previous StateMetaInfo,
            // which may be restored from an old version.
            this.userKeySerializerProvider =
                    StateSerializerProvider.fromNewRegisteredSerializer(newStateSerializer);
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        } else {
            return userKeySerializerProvider.registerNewSerializerForRestoredState(
                    newStateSerializer);
        }
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o)
                && o instanceof RegisteredKeyAndUserKeyValueStateBackendMetaInfo
                && Objects.equals(
                        getUserKeySerializer(),
                        ((RegisteredKeyAndUserKeyValueStateBackendMetaInfo<?, ?, ?>) o)
                                .getUserKeySerializer());
    }

    @Override
    public String toString() {
        return "RegisteredKeyAndUserKeyValueStateBackendMetaInfo{"
                + "stateType="
                + stateType
                + ", name='"
                + name
                + '\''
                + ", namespaceSerializer="
                + getNamespaceSerializer()
                + ", stateSerializer="
                + getStateSerializer()
                + ", userKeySerializer="
                + getUserKeySerializer()
                + '}';
    }

    @Override
    public int hashCode() {
        int result = getName().hashCode();
        result = 31 * result + getStateType().hashCode();
        result = 31 * result + getNamespaceSerializer().hashCode();
        result = 31 * result + getStateSerializer().hashCode();
        if (getUserKeySerializer() != null) {
            result = 31 * result + getUserKeySerializer().hashCode();
        }
        return result;
    }

    @Nonnull
    @Override
    public StateMetaInfoSnapshot snapshot() {
        return computeSnapshot();
    }

    @Nonnull
    @Override
    public RegisteredKeyAndUserKeyValueStateBackendMetaInfo<N, UK, S>
            withSerializerUpgradesAllowed() {
        return new RegisteredKeyAndUserKeyValueStateBackendMetaInfo<>(snapshot());
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

        TypeSerializer<UK> userKeySerializer = getUserKeySerializer();
        if (userKeySerializer != null) {
            String userKeySerializerKey =
                    StateMetaInfoSnapshot.CommonSerializerKeys.USER_KEY_SERIALIZER.toString();
            serializerMap.put(userKeySerializerKey, userKeySerializer.duplicate());
            serializerConfigSnapshotsMap.put(
                    userKeySerializerKey, userKeySerializer.snapshotConfiguration());
        }

        return new StateMetaInfoSnapshot(
                name,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2,
                optionsMap,
                serializerConfigSnapshotsMap,
                serializerMap);
    }
}

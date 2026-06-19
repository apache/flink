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

import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;

/**
 * Utilities for transforming {@link RegisteredKeyValueStateBackendMetaInfo} to {@link
 * org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo}.
 */
public class RegisteredStateMetaInfoUtils {

    private RegisteredStateMetaInfoUtils() {}

    public static org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo
            createMetaInfoV2FromV1Snapshot(StateMetaInfoSnapshot stateMeta) {

        Preconditions.checkArgument(
                stateMeta
                        .getBackendStateType()
                        .equals(StateMetaInfoSnapshot.BackendStateType.KEY_VALUE));

        StateDescriptor.Type newStateType;
        TypeSerializerSnapshot oldValueSerializerSnapshot =
                stateMeta.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
        TypeSerializerSnapshot newUserKeySerializerSnapshot = null;
        TypeSerializerSnapshot newStateSerializerSnapshot = oldValueSerializerSnapshot;

        switch (org.apache.flink.api.common.state.StateDescriptor.Type.valueOf(
                stateMeta.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE))) {
            case VALUE:
                newStateType = StateDescriptor.Type.VALUE;
                break;
            case REDUCING:
                newStateType = StateDescriptor.Type.REDUCING;
                break;
            case AGGREGATING:
                newStateType = StateDescriptor.Type.AGGREGATING;
                break;
            case LIST:
                newStateType = StateDescriptor.Type.LIST;
                newStateSerializerSnapshot =
                        ((ListSerializer) oldValueSerializerSnapshot.restoreSerializer())
                                .getElementSerializer()
                                .snapshotConfiguration();
                break;
            case MAP:
                newStateType = StateDescriptor.Type.MAP;
                newStateSerializerSnapshot =
                        ((MapSerializer) oldValueSerializerSnapshot.restoreSerializer())
                                .getValueSerializer()
                                .snapshotConfiguration();
                newUserKeySerializerSnapshot =
                        ((MapSerializer) oldValueSerializerSnapshot.restoreSerializer())
                                .getKeySerializer()
                                .snapshotConfiguration();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported state type to transform from org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo to org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo");
        }

        if (!newStateType.equals(StateDescriptor.Type.MAP)) {
            return new org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo<>(
                    stateMeta.getName(),
                    newStateType,
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            stateMeta.getTypeSerializerSnapshot(
                                    StateMetaInfoSnapshot.CommonSerializerKeys
                                            .NAMESPACE_SERIALIZER)),
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            newStateSerializerSnapshot),
                    StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        } else {
            return new org.apache.flink.runtime.state.v2
                    .RegisteredKeyAndUserKeyValueStateBackendMetaInfo<>(
                    stateMeta.getName(),
                    newStateType,
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            stateMeta.getTypeSerializerSnapshot(
                                    StateMetaInfoSnapshot.CommonSerializerKeys
                                            .NAMESPACE_SERIALIZER)),
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            newStateSerializerSnapshot),
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            newUserKeySerializerSnapshot),
                    StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        }
    }

    public static RegisteredKeyValueStateBackendMetaInfo createMetaInfoV1FromV2Snapshot(
            StateMetaInfoSnapshot stateMeta) {

        Preconditions.checkArgument(
                stateMeta
                        .getBackendStateType()
                        .equals(StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2));
        org.apache.flink.api.common.state.StateDescriptor.Type newStateType;
        TypeSerializerSnapshot oldValueSerializerSnapshot =
                stateMeta.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
        TypeSerializerSnapshot oldUserKeySerializerSnapshot =
                stateMeta.getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.USER_KEY_SERIALIZER);
        TypeSerializerSnapshot newStateSerializerSnapshot = oldValueSerializerSnapshot;

        switch (org.apache.flink.api.common.state.StateDescriptor.Type.valueOf(
                stateMeta.getOption(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE))) {
            case VALUE:
                newStateType = org.apache.flink.api.common.state.StateDescriptor.Type.VALUE;
                break;
            case REDUCING:
                newStateType = org.apache.flink.api.common.state.StateDescriptor.Type.REDUCING;
                break;
            case AGGREGATING:
                newStateType = org.apache.flink.api.common.state.StateDescriptor.Type.AGGREGATING;
                break;
            case LIST:
                newStateType = org.apache.flink.api.common.state.StateDescriptor.Type.LIST;
                newStateSerializerSnapshot =
                        new ListSerializer<>(oldValueSerializerSnapshot.restoreSerializer())
                                .snapshotConfiguration();
                break;
            case MAP:
                newStateType = org.apache.flink.api.common.state.StateDescriptor.Type.MAP;
                // UserKeySerializerSnapshot may be null since it may be restored from an old
                // version.
                if (oldUserKeySerializerSnapshot == null) {
                    throw new UnsupportedOperationException(
                            "RegisteredKeyAndUserKeyValueStateBackendMetaInfo can not transform to org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo since oldUserKeySerializerSnapshot is null");
                } else {
                    newStateSerializerSnapshot =
                            new MapSerializer<>(
                                            oldUserKeySerializerSnapshot.restoreSerializer(),
                                            oldValueSerializerSnapshot.restoreSerializer())
                                    .snapshotConfiguration();
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported state type to transform from org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo to org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo");
        }
        return new RegisteredKeyValueStateBackendMetaInfo<>(
                newStateType,
                stateMeta.getName(),
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        stateMeta.getTypeSerializerSnapshot(
                                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER)),
                StateSerializerProvider.fromPreviousSerializerSnapshot(newStateSerializerSnapshot),
                StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
    }
}

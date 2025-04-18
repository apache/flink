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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;

/**
 * Utilities for transforming {@link RegisteredKeyValueStateBackendMetaInfo} to {@link
 * org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo}.
 */
public class RegisteredStateMetaInfoUtils {

    private RegisteredStateMetaInfoUtils() {}

    public static org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo
            transformFromV1ToV2(RegisteredKeyValueStateBackendMetaInfo metaInfoV1) {
        StateDescriptor.Type newStateType;
        TypeSerializer newStateSerializer = metaInfoV1.getStateSerializer();
        TypeSerializer newUserKeySerializer = null;

        switch (metaInfoV1.getStateType()) {
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
                newStateSerializer =
                        ((ListSerializer) metaInfoV1.getStateSerializer()).getElementSerializer();
                break;
            case MAP:
                newStateType = StateDescriptor.Type.MAP;
                newStateSerializer =
                        ((MapSerializer) metaInfoV1.getStateSerializer()).getValueSerializer();
                newUserKeySerializer =
                        ((MapSerializer) metaInfoV1.getStateSerializer()).getKeySerializer();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported state type to transform from org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo to org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo");
        }

        if (!newStateType.equals(StateDescriptor.Type.MAP)) {
            return new org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo<>(
                    metaInfoV1.getName(),
                    newStateType,
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            metaInfoV1.getNamespaceSerializer().snapshotConfiguration()),
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            newStateSerializer.snapshotConfiguration()),
                    StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        } else {
            return new org.apache.flink.runtime.state.v2
                    .RegisteredKeyAndUserKeyValueStateBackendMetaInfo<>(
                    metaInfoV1.getName(),
                    newStateType,
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            metaInfoV1.getNamespaceSerializer().snapshotConfiguration()),
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            newStateSerializer.snapshotConfiguration()),
                    StateSerializerProvider.fromPreviousSerializerSnapshot(
                            newUserKeySerializer.snapshotConfiguration()),
                    StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        }
    }

    public static RegisteredKeyValueStateBackendMetaInfo transformFromV2ToV1(
            org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo metaInfoV1) {
        org.apache.flink.api.common.state.StateDescriptor.Type newStateType;
        TypeSerializer newStateSerializer = metaInfoV1.getStateSerializer();

        switch (metaInfoV1.getStateType()) {
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
                newStateSerializer = new ListSerializer<>(metaInfoV1.getStateSerializer());
                break;
            case MAP:
                newStateType = org.apache.flink.api.common.state.StateDescriptor.Type.MAP;
                // UserKeySerializer may be null since it may be restored from an old version.
                TypeSerializer userKeySerializer =
                        metaInfoV1
                                        instanceof
                                        org.apache.flink.runtime.state.v2
                                                .RegisteredKeyAndUserKeyValueStateBackendMetaInfo
                                ? ((org.apache.flink.runtime.state.v2
                                                        .RegisteredKeyAndUserKeyValueStateBackendMetaInfo)
                                                metaInfoV1)
                                        .getUserKeySerializer()
                                : null;
                if (userKeySerializer == null) {
                    throw new UnsupportedOperationException(
                            "RegisteredKeyAndUserKeyValueStateBackendMetaInfo can not transform to org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo since userKeySerializer is null");
                } else {
                    newStateSerializer =
                            new MapSerializer<>(userKeySerializer, metaInfoV1.getStateSerializer());
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported state type to transform from org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo to org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo");
        }
        return new RegisteredKeyValueStateBackendMetaInfo<>(
                newStateType,
                metaInfoV1.getName(),
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        metaInfoV1.getNamespaceSerializer().snapshotConfiguration()),
                StateSerializerProvider.fromPreviousSerializerSnapshot(
                        newStateSerializer.snapshotConfiguration()),
                StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
    }
}

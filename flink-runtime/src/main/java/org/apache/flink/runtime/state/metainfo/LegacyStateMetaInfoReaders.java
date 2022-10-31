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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.OperatorStateHandle;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class holds the deprecated implementations of readers for state meta infos. They can be
 * removed when we drop backwards compatibility.
 */
public class LegacyStateMetaInfoReaders {

    private LegacyStateMetaInfoReaders() {}

    /**
     * Implementation of {@link StateMetaInfoReader} for version 3 of keyed state. - v3: Flink
     * 1.4.x, 1.5.x
     */
    static class KeyedBackendStateMetaInfoReaderV3V4 implements StateMetaInfoReader {

        static final KeyedBackendStateMetaInfoReaderV3V4 INSTANCE =
                new KeyedBackendStateMetaInfoReaderV3V4();

        @Nonnull
        @Override
        public StateMetaInfoSnapshot readStateMetaInfoSnapshot(
                @Nonnull DataInputView in, @Nonnull ClassLoader userCodeClassLoader)
                throws IOException {

            final StateDescriptor.Type stateDescType = StateDescriptor.Type.values()[in.readInt()];
            final String stateName = in.readUTF();
            List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializersAndConfigs =
                    TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(
                            in, userCodeClassLoader);

            Map<String, String> optionsMap =
                    Collections.singletonMap(
                            StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
                            stateDescType.toString());

            Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotMap = new HashMap<>(2);
            serializerConfigSnapshotMap.put(
                    StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString(),
                    serializersAndConfigs.get(0).f1);
            serializerConfigSnapshotMap.put(
                    StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
                    serializersAndConfigs.get(1).f1);

            return new StateMetaInfoSnapshot(
                    stateName,
                    StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                    optionsMap,
                    serializerConfigSnapshotMap);
        }
    }

    /**
     * Unified reader for older versions of operator (version 2 and 3) AND broadcast state (version
     * 3).
     *
     * <p>- v2: Flink 1.3.x, 1.4.x - v3: Flink 1.5.x
     */
    static class OperatorBackendStateMetaInfoReaderV2V3 implements StateMetaInfoReader {

        static final OperatorBackendStateMetaInfoReaderV2V3 INSTANCE =
                new OperatorBackendStateMetaInfoReaderV2V3();

        @Nonnull
        @Override
        public StateMetaInfoSnapshot readStateMetaInfoSnapshot(
                @Nonnull DataInputView in, @Nonnull ClassLoader userCodeClassLoader)
                throws IOException {

            final String name = in.readUTF();
            final OperatorStateHandle.Mode mode = OperatorStateHandle.Mode.values()[in.readByte()];

            Map<String, String> optionsMap =
                    Collections.singletonMap(
                            StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE
                                    .toString(),
                            mode.toString());

            List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>>
                    stateSerializerAndConfigList =
                            TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(
                                    in, userCodeClassLoader);

            final int listSize = stateSerializerAndConfigList.size();
            StateMetaInfoSnapshot.BackendStateType stateType =
                    listSize == 1
                            ? StateMetaInfoSnapshot.BackendStateType.OPERATOR
                            : StateMetaInfoSnapshot.BackendStateType.BROADCAST;

            Map<String, TypeSerializerSnapshot<?>> serializerConfigsMap = new HashMap<>(listSize);
            switch (stateType) {
                case OPERATOR:
                    serializerConfigsMap.put(
                            StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
                            stateSerializerAndConfigList.get(0).f1);
                    break;
                case BROADCAST:
                    serializerConfigsMap.put(
                            StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString(),
                            stateSerializerAndConfigList.get(0).f1);

                    serializerConfigsMap.put(
                            StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
                            stateSerializerAndConfigList.get(1).f1);
                    break;
                default:
                    throw new IllegalStateException("Unknown operator state type " + stateType);
            }

            return new StateMetaInfoSnapshot(name, stateType, optionsMap, serializerConfigsMap);
        }
    }
}

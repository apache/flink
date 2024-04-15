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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test fixes the enum constants in {@link StateMetaInfoSnapshot} because any changes can break
 * backwards compatibility. Consider this before changing this test.
 */
class StateMetaInfoSnapshotEnumConstantsTest {

    @Test
    void testFixedBackendStateTypeEnumConstants() {
        assertThat(StateMetaInfoSnapshot.BackendStateType.values()).hasSize(4);
        assertThat(StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.ordinal()).isZero();
        assertThat(StateMetaInfoSnapshot.BackendStateType.OPERATOR.ordinal()).isOne();
        assertThat(StateMetaInfoSnapshot.BackendStateType.BROADCAST.ordinal()).isEqualTo(2);
        assertThat(StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE.ordinal()).isEqualTo(3);
        assertThat(StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.toString())
                .isEqualTo("KEY_VALUE");
        assertThat(StateMetaInfoSnapshot.BackendStateType.OPERATOR.toString())
                .isEqualTo("OPERATOR");
        assertThat(StateMetaInfoSnapshot.BackendStateType.BROADCAST.toString())
                .isEqualTo("BROADCAST");
        assertThat(StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE.toString())
                .isEqualTo("PRIORITY_QUEUE");
    }

    @Test
    void testFixedOptionsEnumConstants() {
        assertThat(StateMetaInfoSnapshot.CommonOptionsKeys.values()).hasSize(2);
        assertThat(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.ordinal()).isZero();
        assertThat(
                        StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE
                                .ordinal())
                .isOne();
        assertThat(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString())
                .isEqualTo("KEYED_STATE_TYPE");
        assertThat(
                        StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE
                                .toString())
                .isEqualTo("OPERATOR_STATE_DISTRIBUTION_MODE");
    }

    @Test
    void testFixedSerializerEnumConstants() {
        assertThat(StateMetaInfoSnapshot.CommonSerializerKeys.values()).hasSize(3);
        assertThat(StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.ordinal()).isZero();
        assertThat(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.ordinal())
                .isOne();
        assertThat(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.ordinal())
                .isEqualTo(2);
        assertThat(StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString())
                .isEqualTo("KEY_SERIALIZER");
        assertThat(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString())
                .isEqualTo("NAMESPACE_SERIALIZER");
        assertThat(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString())
                .isEqualTo("VALUE_SERIALIZER");
    }
}

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
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This test fixes the enum constants in {@link StateMetaInfoSnapshot} because any changes can break
 * backwards compatibility. Consider this before changing this test.
 */
public class StateMetaInfoSnapshotEnumConstantsTest {

    @Test
    public void testFixedBackendStateTypeEnumConstants() {
        Assertions.assertEquals(4, StateMetaInfoSnapshot.BackendStateType.values().length);
        Assertions.assertEquals(0, StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.ordinal());
        Assertions.assertEquals(1, StateMetaInfoSnapshot.BackendStateType.OPERATOR.ordinal());
        Assertions.assertEquals(2, StateMetaInfoSnapshot.BackendStateType.BROADCAST.ordinal());
        Assertions.assertEquals(3, StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE.ordinal());
        Assertions.assertEquals(
                "KEY_VALUE", StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.toString());
        Assertions.assertEquals(
                "OPERATOR", StateMetaInfoSnapshot.BackendStateType.OPERATOR.toString());
        Assertions.assertEquals(
                "BROADCAST", StateMetaInfoSnapshot.BackendStateType.BROADCAST.toString());
        Assertions.assertEquals(
                "PRIORITY_QUEUE", StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE.toString());
    }

    @Test
    public void testFixedOptionsEnumConstants() {
        Assertions.assertEquals(2, StateMetaInfoSnapshot.CommonOptionsKeys.values().length);
        Assertions.assertEquals(
                0, StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.ordinal());
        Assertions.assertEquals(
                1,
                StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.ordinal());
        Assertions.assertEquals(
                "KEYED_STATE_TYPE",
                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString());
        Assertions.assertEquals(
                "OPERATOR_STATE_DISTRIBUTION_MODE",
                StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE
                        .toString());
    }

    @Test
    public void testFixedSerializerEnumConstants() {
        Assertions.assertEquals(3, StateMetaInfoSnapshot.CommonSerializerKeys.values().length);
        Assertions.assertEquals(
                0, StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.ordinal());
        Assertions.assertEquals(
                1, StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.ordinal());
        Assertions.assertEquals(
                2, StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.ordinal());
        Assertions.assertEquals(
                "KEY_SERIALIZER",
                StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString());
        Assertions.assertEquals(
                "NAMESPACE_SERIALIZER",
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString());
        Assertions.assertEquals(
                "VALUE_SERIALIZER",
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString());
    }
}

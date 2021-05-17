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

import org.junit.Assert;
import org.junit.Test;

/**
 * This test fixes the enum constants in {@link StateMetaInfoSnapshot} because any changes can break
 * backwards compatibility. Consider this before changing this test.
 */
public class StateMetaInfoSnapshotEnumConstantsTest {

    @Test
    public void testFixedBackendStateTypeEnumConstants() {
        Assert.assertEquals(4, StateMetaInfoSnapshot.BackendStateType.values().length);
        Assert.assertEquals(0, StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.ordinal());
        Assert.assertEquals(1, StateMetaInfoSnapshot.BackendStateType.OPERATOR.ordinal());
        Assert.assertEquals(2, StateMetaInfoSnapshot.BackendStateType.BROADCAST.ordinal());
        Assert.assertEquals(3, StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE.ordinal());
        Assert.assertEquals(
                "KEY_VALUE", StateMetaInfoSnapshot.BackendStateType.KEY_VALUE.toString());
        Assert.assertEquals("OPERATOR", StateMetaInfoSnapshot.BackendStateType.OPERATOR.toString());
        Assert.assertEquals(
                "BROADCAST", StateMetaInfoSnapshot.BackendStateType.BROADCAST.toString());
        Assert.assertEquals(
                "PRIORITY_QUEUE", StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE.toString());
    }

    @Test
    public void testFixedOptionsEnumConstants() {
        Assert.assertEquals(2, StateMetaInfoSnapshot.CommonOptionsKeys.values().length);
        Assert.assertEquals(0, StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.ordinal());
        Assert.assertEquals(
                1,
                StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.ordinal());
        Assert.assertEquals(
                "KEYED_STATE_TYPE",
                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString());
        Assert.assertEquals(
                "OPERATOR_STATE_DISTRIBUTION_MODE",
                StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE
                        .toString());
    }

    @Test
    public void testFixedSerializerEnumConstants() {
        Assert.assertEquals(3, StateMetaInfoSnapshot.CommonSerializerKeys.values().length);
        Assert.assertEquals(0, StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.ordinal());
        Assert.assertEquals(
                1, StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.ordinal());
        Assert.assertEquals(
                2, StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.ordinal());
        Assert.assertEquals(
                "KEY_SERIALIZER",
                StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString());
        Assert.assertEquals(
                "NAMESPACE_SERIALIZER",
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString());
        Assert.assertEquals(
                "VALUE_SERIALIZER",
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString());
    }
}

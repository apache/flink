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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.MethodForwardingTestUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

/** Tests for {@link StateObjectCollection}. */
public class StateObjectCollectionTest extends TestLogger {

    @Test
    public void testEmptyCollection() {
        StateObjectCollection<StateObject> empty = StateObjectCollection.empty();
        Assert.assertEquals(0, empty.getStateSize());
    }

    @Test
    public void testForwardingCollectionMethods() throws Exception {
        MethodForwardingTestUtil.testMethodForwarding(
                Collection.class,
                ((Function<Collection, StateObjectCollection>) StateObjectCollection::new));
    }

    @Test
    public void testForwardingStateObjectMethods() throws Exception {
        MethodForwardingTestUtil.testMethodForwarding(
                StateObject.class,
                object -> new StateObjectCollection<>(Collections.singletonList(object)));
    }

    @Test
    public void testHasState() {
        StateObjectCollection<StateObject> stateObjects =
                new StateObjectCollection<>(new ArrayList<>());
        Assert.assertFalse(stateObjects.hasState());

        stateObjects = new StateObjectCollection<>(Collections.singletonList(null));
        Assert.assertFalse(stateObjects.hasState());

        stateObjects =
                new StateObjectCollection<>(Collections.singletonList(mock(StateObject.class)));
        Assert.assertTrue(stateObjects.hasState());
    }
}

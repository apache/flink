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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link StateObjectCollection}. */
class StateObjectCollectionTest {

    @Test
    void testEmptyCollection() {
        StateObjectCollection<StateObject> empty = StateObjectCollection.empty();
        assertThat(empty.getStateSize()).isZero();
    }

    @Test
    void testForwardingCollectionMethods() throws Exception {
        MethodForwardingTestUtil.testMethodForwarding(
                Collection.class,
                ((Function<Collection, StateObjectCollection>) StateObjectCollection::new));
    }

    @Test
    void testForwardingStateObjectMethods() throws Exception {
        MethodForwardingTestUtil.testMethodForwarding(
                StateObject.class,
                object -> new StateObjectCollection<>(Collections.singletonList(object)));
    }

    @Test
    void testHasState() {
        StateObjectCollection<StateObject> stateObjects =
                new StateObjectCollection<>(new ArrayList<>());
        assertThat(stateObjects.hasState()).isFalse();

        stateObjects = new StateObjectCollection<>(Collections.singletonList(null));
        assertThat(stateObjects.hasState()).isFalse();

        stateObjects =
                new StateObjectCollection<>(Collections.singletonList(mock(StateObject.class)));
        assertThat(stateObjects.hasState()).isTrue();
    }
}

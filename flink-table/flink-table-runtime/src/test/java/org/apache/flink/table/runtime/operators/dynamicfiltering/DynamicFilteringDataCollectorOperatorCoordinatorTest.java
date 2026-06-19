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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TestingOperatorCoordinator;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DynamicFilteringDataCollectorOperatorCoordinator}. */
class DynamicFilteringDataCollectorOperatorCoordinatorTest {

    @Test
    void testRedistributeData() throws Exception {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(new OperatorID(), 1);
        String listenerID1 = "test-listener-1";
        String listenerID2 = "test-listener-2";

        TestingOperatorCoordinator listener1 = new TestingOperatorCoordinator(context);
        TestingOperatorCoordinator listener2 = new TestingOperatorCoordinator(context);
        context.getCoordinatorStore().putIfAbsent(listenerID1, listener1);
        context.getCoordinatorStore().putIfAbsent(listenerID2, listener2);

        RowType rowType = RowType.of(new IntType());
        OperatorEvent testEvent = dynamicFilteringEvent(rowType, Collections.emptyList());
        try (DynamicFilteringDataCollectorOperatorCoordinator coordinator =
                new DynamicFilteringDataCollectorOperatorCoordinator(
                        context, Arrays.asList(listenerID1, listenerID2))) {
            coordinator.handleEventFromOperator(0, 1, testEvent);
        }

        assertThat(listener1.getNextReceivedOperatorEvent()).isSameAs(testEvent);
        assertThat(listener1.getNextReceivedOperatorEvent()).isNull();
        assertThat(listener2.getNextReceivedOperatorEvent()).isSameAs(testEvent);
        assertThat(listener2.getNextReceivedOperatorEvent()).isNull();
    }

    @Test
    void testTaskFailover() throws Exception {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(new OperatorID(), 1);
        String listenerID = "test-listener-1";

        TestingOperatorCoordinator listener = new TestingOperatorCoordinator(context);
        context.getCoordinatorStore().putIfAbsent(listenerID, listener);

        RowType rowType = RowType.of(new IntType());
        try (DynamicFilteringDataCollectorOperatorCoordinator coordinator =
                new DynamicFilteringDataCollectorOperatorCoordinator(
                        context, Arrays.asList(listenerID))) {
            OperatorEvent testEvent =
                    dynamicFilteringEvent(rowType, Collections.singletonList(new byte[] {1, 2}));
            coordinator.handleEventFromOperator(0, 0, testEvent);
            assertThat(listener.getNextReceivedOperatorEvent()).isSameAs(testEvent);

            // failover happens
            coordinator.executionAttemptFailed(0, 0, null);

            OperatorEvent testEvent1 =
                    dynamicFilteringEvent(rowType, Collections.singletonList(new byte[] {1, 2}));
            coordinator.handleEventFromOperator(0, 1, testEvent1);
            // testEvent1 contains the same data as testEvent, nothing happens and don't send event
            assertThat(listener.getNextReceivedOperatorEvent()).isNull();

            // failover happens again
            coordinator.executionAttemptFailed(0, 1, null);

            OperatorEvent testEvent2 =
                    dynamicFilteringEvent(rowType, Collections.singletonList(new byte[] {1, 3}));
            assertThatThrownBy(() -> coordinator.handleEventFromOperator(0, 2, testEvent2))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    private OperatorEvent dynamicFilteringEvent(RowType rowType, List<byte[]> data) {
        return new SourceEventWrapper(
                new DynamicFilteringEvent(
                        new DynamicFilteringData(
                                InternalTypeInfo.of(rowType), rowType, data, data.isEmpty())));
    }
}

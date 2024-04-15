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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks.EventWithSubtask;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link SubtaskGatewayImpl}. */
class SubtaskGatewayImplTest {

    @Test
    void eventsPassThroughOpenGateway() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final SubtaskGatewayImpl gateway =
                new SubtaskGatewayImpl(
                        getUniqueElement(receiver.getAccessesForSubtask(11)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new IncompleteFuturesTracker());

        final OperatorEvent event = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future = gateway.sendEvent(event);

        assertThat(receiver.events).containsExactly(new EventWithSubtask(event, 11));
        assertThat(future).isDone();
    }

    @Test
    void closingMarkedGateway() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final SubtaskGatewayImpl gateway =
                new SubtaskGatewayImpl(
                        getUniqueElement(receiver.getAccessesForSubtask(11)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new IncompleteFuturesTracker());

        gateway.markForCheckpoint(200L);
        final boolean isClosed = gateway.tryCloseGateway(200L);

        assertThat(isClosed).isTrue();
    }

    @Test
    void notClosingUnmarkedGateway() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final SubtaskGatewayImpl gateway =
                new SubtaskGatewayImpl(
                        getUniqueElement(receiver.getAccessesForSubtask(11)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new IncompleteFuturesTracker());

        final boolean isClosed = gateway.tryCloseGateway(123L);

        assertThat(isClosed).isFalse();
    }

    @Test
    void notClosingGatewayForOtherMark() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final SubtaskGatewayImpl gateway =
                new SubtaskGatewayImpl(
                        getUniqueElement(receiver.getAccessesForSubtask(11)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new IncompleteFuturesTracker());

        gateway.markForCheckpoint(100L);
        final boolean isClosed = gateway.tryCloseGateway(123L);

        assertThat(isClosed).isFalse();
    }

    @Test
    void eventsBlockedByClosedGateway() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final SubtaskGatewayImpl gateway =
                new SubtaskGatewayImpl(
                        getUniqueElement(receiver.getAccessesForSubtask(1)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new IncompleteFuturesTracker());

        gateway.markForCheckpoint(1L);
        gateway.tryCloseGateway(1L);

        final CompletableFuture<Acknowledge> future = gateway.sendEvent(new TestOperatorEvent());

        assertThat(receiver.events).isEmpty();
        assertThat(future).isNotDone();
    }

    @Test
    void eventsReleasedAfterOpeningGateway() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final SubtaskGatewayImpl gateway0 =
                new SubtaskGatewayImpl(
                        getUniqueElement(receiver.getAccessesForSubtask(0)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new IncompleteFuturesTracker());
        final SubtaskGatewayImpl gateway3 =
                new SubtaskGatewayImpl(
                        getUniqueElement(receiver.getAccessesForSubtask(3)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new IncompleteFuturesTracker());
        List<SubtaskGatewayImpl> gateways = Arrays.asList(gateway3, gateway0);

        gateways.forEach(x -> x.markForCheckpoint(17L));
        gateways.forEach(x -> x.tryCloseGateway(17L));

        final OperatorEvent event1 = new TestOperatorEvent();
        final OperatorEvent event2 = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future1 = gateway3.sendEvent(event1);
        final CompletableFuture<Acknowledge> future2 = gateway0.sendEvent(event2);

        gateways.forEach(SubtaskGatewayImpl::openGatewayAndUnmarkAllCheckpoint);

        assertThat(receiver.events)
                .containsExactly(new EventWithSubtask(event1, 3), new EventWithSubtask(event2, 0));
        assertThat(future1).isDone();
        assertThat(future2).isDone();
    }

    @Test
    void releasedEventsForwardSendFailures() {
        final EventReceivingTasks receiver =
                EventReceivingTasks.createForRunningTasksFailingRpcs(new FlinkException("test"));
        final SubtaskGatewayImpl gateway =
                new SubtaskGatewayImpl(
                        getUniqueElement(receiver.getAccessesForSubtask(10)),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new IncompleteFuturesTracker());

        gateway.markForCheckpoint(17L);
        gateway.tryCloseGateway(17L);

        final CompletableFuture<Acknowledge> future = gateway.sendEvent(new TestOperatorEvent());
        gateway.openGatewayAndUnmarkAllCheckpoint();

        assertThat(future).isCompletedExceptionally();
    }

    private static <T> T getUniqueElement(Collection<T> collection) {
        Iterator<T> iterator = collection.iterator();
        T element = iterator.next();
        Preconditions.checkState(!iterator.hasNext());
        return element;
    }
}

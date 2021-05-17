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

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks.EventWithSubtask;
import org.apache.flink.util.FlinkException;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Unit tests for the {@link OperatorEventValve}. */
public class OperatorEventValveTest {

    @Test
    public void eventsPassThroughOpenValve() {
        final EventReceivingTasks sender = EventReceivingTasks.createForRunningTasks();
        final OperatorEventValve valve = new OperatorEventValve();

        final OperatorEvent event = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future = new CompletableFuture<>();
        valve.sendEvent(sender.createSendAction(event, 11), future);

        assertThat(sender.events, contains(new EventWithSubtask(event, 11)));
        assertTrue(future.isDone());
    }

    @Test
    public void shuttingMarkedValve() {
        final OperatorEventValve valve = new OperatorEventValve();

        valve.markForCheckpoint(200L);
        final boolean shut = valve.tryShutValve(200L);

        assertTrue(shut);
    }

    @Test
    public void notShuttingUnmarkedValve() {
        final OperatorEventValve valve = new OperatorEventValve();

        final boolean shut = valve.tryShutValve(123L);

        assertFalse(shut);
    }

    @Test
    public void notShuttingValveForOtherMark() {
        final OperatorEventValve valve = new OperatorEventValve();

        valve.markForCheckpoint(100L);
        final boolean shut = valve.tryShutValve(123L);

        assertFalse(shut);
    }

    @Test
    public void eventsBlockedByClosedValve() {
        final EventReceivingTasks sender = EventReceivingTasks.createForRunningTasks();
        final OperatorEventValve valve = new OperatorEventValve();

        valve.markForCheckpoint(1L);
        valve.tryShutValve(1L);

        final CompletableFuture<Acknowledge> future = new CompletableFuture<>();
        valve.sendEvent(sender.createSendAction(new TestOperatorEvent(), 1), future);

        assertTrue(sender.events.isEmpty());
        assertFalse(future.isDone());
    }

    @Test
    public void eventsReleasedAfterOpeningValve() {
        final EventReceivingTasks sender = EventReceivingTasks.createForRunningTasks();
        final OperatorEventValve valve = new OperatorEventValve();

        valve.markForCheckpoint(17L);
        valve.tryShutValve(17L);

        final OperatorEvent event1 = new TestOperatorEvent();
        final OperatorEvent event2 = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future1 = new CompletableFuture<>();
        valve.sendEvent(sender.createSendAction(event1, 3), future1);
        final CompletableFuture<Acknowledge> future2 = new CompletableFuture<>();
        valve.sendEvent(sender.createSendAction(event2, 0), future2);

        valve.openValveAndUnmarkCheckpoint();

        assertThat(
                sender.events,
                contains(new EventWithSubtask(event1, 3), new EventWithSubtask(event2, 0)));
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
    }

    @Test
    public void releasedEventsForwardSendFailures() {
        final EventReceivingTasks sender =
                EventReceivingTasks.createForRunningTasksFailingRpcs(new FlinkException("test"));
        final OperatorEventValve valve = new OperatorEventValve();

        valve.markForCheckpoint(17L);
        valve.tryShutValve(17L);

        final CompletableFuture<Acknowledge> future = new CompletableFuture<>();
        valve.sendEvent(sender.createSendAction(new TestOperatorEvent(), 10), future);
        valve.openValveAndUnmarkCheckpoint();

        assertTrue(future.isCompletedExceptionally());
    }
}

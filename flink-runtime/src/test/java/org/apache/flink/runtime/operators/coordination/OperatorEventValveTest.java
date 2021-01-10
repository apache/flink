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
import org.apache.flink.runtime.operators.coordination.TestEventSender.EventWithSubtask;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Unit tests for the {@link OperatorEventValve}. */
public class OperatorEventValveTest {

    @Test
    public void eventsPassThroughOpenValve() throws Exception {
        final TestEventSender sender = new TestEventSender();
        final OperatorEventValve valve = new OperatorEventValve(sender);

        final OperatorEvent event = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future =
                valve.sendEvent(new SerializedValue<>(event), 11);

        assertThat(sender.events, contains(new EventWithSubtask(event, 11)));
        assertTrue(future.isDone());
    }

    @Test(expected = IllegalStateException.class)
    public void errorShuttingUnmarkedValve() throws Exception {
        final TestEventSender sender = new TestEventSender();
        final OperatorEventValve valve = new OperatorEventValve(sender);

        valve.shutValve(123L);
    }

    @Test(expected = IllegalStateException.class)
    public void errorShuttingValveForOtherMark() throws Exception {
        final TestEventSender sender = new TestEventSender();
        final OperatorEventValve valve = new OperatorEventValve(sender);

        valve.markForCheckpoint(100L);
        valve.shutValve(123L);
    }

    @Test
    public void eventsBlockedByClosedValve() throws Exception {
        final TestEventSender sender = new TestEventSender();
        final OperatorEventValve valve = new OperatorEventValve(sender);

        valve.markForCheckpoint(1L);
        valve.shutValve(1L);

        final CompletableFuture<Acknowledge> future =
                valve.sendEvent(new SerializedValue<>(new TestOperatorEvent()), 1);

        assertTrue(sender.events.isEmpty());
        assertFalse(future.isDone());
    }

    @Test
    public void eventsReleasedAfterOpeningValve() throws Exception {
        final TestEventSender sender = new TestEventSender();
        final OperatorEventValve valve = new OperatorEventValve(sender);

        valve.markForCheckpoint(17L);
        valve.shutValve(17L);

        final OperatorEvent event1 = new TestOperatorEvent();
        final OperatorEvent event2 = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future1 =
                valve.sendEvent(new SerializedValue<>(event1), 3);
        final CompletableFuture<Acknowledge> future2 =
                valve.sendEvent(new SerializedValue<>(event2), 0);

        valve.openValveAndUnmarkCheckpoint();

        assertThat(
                sender.events,
                containsInAnyOrder(
                        new EventWithSubtask(event1, 3), new EventWithSubtask(event2, 0)));
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
    }

    @Test
    public void releasedEventsForwardSendFailures() throws Exception {
        final TestEventSender sender = new TestEventSender(new FlinkException("test"));
        final OperatorEventValve valve = new OperatorEventValve(sender);

        valve.markForCheckpoint(17L);
        valve.shutValve(17L);

        final CompletableFuture<Acknowledge> future =
                valve.sendEvent(new SerializedValue<>(new TestOperatorEvent()), 10);
        valve.openValveAndUnmarkCheckpoint();

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void resetDropsAllEvents() throws Exception {
        final TestEventSender sender = new TestEventSender();
        final OperatorEventValve valve = new OperatorEventValve(sender);
        valve.markForCheckpoint(17L);
        valve.shutValve(17L);

        valve.sendEvent(new SerializedValue<>(new TestOperatorEvent()), 0);
        valve.sendEvent(new SerializedValue<>(new TestOperatorEvent()), 1);

        valve.reset();
        valve.openValveAndUnmarkCheckpoint();

        assertTrue(sender.events.isEmpty());
    }

    @Test
    public void resetForTaskDropsSelectiveEvents() throws Exception {
        final TestEventSender sender = new TestEventSender();
        final OperatorEventValve valve = new OperatorEventValve(sender);
        valve.markForCheckpoint(17L);
        valve.shutValve(17L);

        final OperatorEvent event1 = new TestOperatorEvent();
        final OperatorEvent event2 = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future1 =
                valve.sendEvent(new SerializedValue<>(event1), 0);
        final CompletableFuture<Acknowledge> future2 =
                valve.sendEvent(new SerializedValue<>(event2), 1);

        valve.resetForTask(1);
        valve.openValveAndUnmarkCheckpoint();

        assertThat(sender.events, contains(new EventWithSubtask(event1, 0)));
        assertTrue(future1.isDone());
        assertTrue(future2.isCompletedExceptionally());
    }

    @Test
    public void resetOpensValve() throws Exception {
        final TestEventSender sender = new TestEventSender();
        final OperatorEventValve valve = new OperatorEventValve(sender);

        valve.markForCheckpoint(17L);
        valve.shutValve(17L);
        valve.reset();

        assertFalse(valve.isShut());
    }
}

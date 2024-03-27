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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.iterative.event.AllWorkersDoneEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.util.event.EventListener;

import org.junit.jupiter.api.Test;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Basic tests for {@link TaskEventDispatcher}. */
class TaskEventDispatcherTest {

    @Test
    void registerPartitionTwice() {
        ResultPartitionID partitionId = new ResultPartitionID();
        TaskEventDispatcher ted = new TaskEventDispatcher();
        ted.registerPartition(partitionId);

        assertThatThrownBy(() -> ted.registerPartition(partitionId))
                .hasMessageContaining("already registered at task event dispatcher")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void subscribeToEventNotRegistered() {
        TaskEventDispatcher ted = new TaskEventDispatcher();
        assertThatThrownBy(
                        () ->
                                ted.subscribeToEvent(
                                        new ResultPartitionID(),
                                        new ZeroShotEventListener(),
                                        TaskEvent.class))
                .hasMessageContaining("not registered at task event dispatcher")
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Tests {@link TaskEventDispatcher#publish(ResultPartitionID, TaskEvent)} and {@link
     * TaskEventDispatcher#subscribeToEvent(ResultPartitionID, EventListener, Class)} methods.
     */
    @Test
    void publishSubscribe() {
        ResultPartitionID partitionId1 = new ResultPartitionID();
        ResultPartitionID partitionId2 = new ResultPartitionID();
        TaskEventDispatcher ted = new TaskEventDispatcher();

        AllWorkersDoneEvent event1 = new AllWorkersDoneEvent();
        TerminationEvent event2 = new TerminationEvent();
        assertThat(ted.publish(partitionId1, event1)).isFalse();

        ted.registerPartition(partitionId1);
        ted.registerPartition(partitionId2);

        // no event listener subscribed yet, but the event is forwarded to a TaskEventHandler
        assertThat(ted.publish(partitionId1, event1)).isTrue();

        OneShotEventListener eventListener1a = new OneShotEventListener(event1);
        ZeroShotEventListener eventListener1b = new ZeroShotEventListener();
        ZeroShotEventListener eventListener2 = new ZeroShotEventListener();
        OneShotEventListener eventListener3 = new OneShotEventListener(event2);
        ted.subscribeToEvent(partitionId1, eventListener1a, AllWorkersDoneEvent.class);
        ted.subscribeToEvent(partitionId2, eventListener1b, AllWorkersDoneEvent.class);
        ted.subscribeToEvent(partitionId1, eventListener2, TaskEvent.class);
        ted.subscribeToEvent(partitionId1, eventListener3, TerminationEvent.class);

        assertThat(ted.publish(partitionId1, event1)).isTrue();
        assertThat(eventListener1a.fired)
                .withFailMessage("listener should have fired for AllWorkersDoneEvent")
                .isTrue();
        assertThat(eventListener3.fired)
                .withFailMessage("listener should not have fired for AllWorkersDoneEvent")
                .isFalse();

        // publish another event, verify that only the right subscriber is called
        assertThat(ted.publish(partitionId1, event2)).isTrue();
        assertThat(eventListener3.fired)
                .withFailMessage("listener should have fired for TerminationEvent")
                .isTrue();
    }

    @Test
    void unregisterPartition() {
        ResultPartitionID partitionId1 = new ResultPartitionID();
        ResultPartitionID partitionId2 = new ResultPartitionID();
        TaskEventDispatcher ted = new TaskEventDispatcher();

        AllWorkersDoneEvent event = new AllWorkersDoneEvent();
        assertThat(ted.publish(partitionId1, event)).isFalse();

        ted.registerPartition(partitionId1);
        ted.registerPartition(partitionId2);

        OneShotEventListener eventListener1a = new OneShotEventListener(event);
        ZeroShotEventListener eventListener1b = new ZeroShotEventListener();
        OneShotEventListener eventListener2 = new OneShotEventListener(event);
        ted.subscribeToEvent(partitionId1, eventListener1a, AllWorkersDoneEvent.class);
        ted.subscribeToEvent(partitionId2, eventListener1b, AllWorkersDoneEvent.class);
        ted.subscribeToEvent(partitionId1, eventListener2, AllWorkersDoneEvent.class);

        ted.unregisterPartition(partitionId2);

        // publish something for partitionId1 triggering all according listeners
        assertThat(ted.publish(partitionId1, event)).isTrue();
        assertThat(eventListener1a.fired)
                .withFailMessage("listener should have fired for AllWorkersDoneEvent")
                .isTrue();
        assertThat(eventListener2.fired)
                .withFailMessage("listener should have fired for AllWorkersDoneEvent")
                .isTrue();

        // now publish something for partitionId2 which should not trigger any listeners
        assertThat(ted.publish(partitionId2, event)).isFalse();
    }

    @Test
    void clearAll() throws Exception {
        ResultPartitionID partitionId = new ResultPartitionID();
        TaskEventDispatcher ted = new TaskEventDispatcher();
        ted.registerPartition(partitionId);

        //noinspection unchecked
        ZeroShotEventListener eventListener1 = new ZeroShotEventListener();
        ted.subscribeToEvent(partitionId, eventListener1, AllWorkersDoneEvent.class);

        ted.clearAll();

        assertThat(ted.publish(partitionId, new AllWorkersDoneEvent())).isFalse();
    }

    /**
     * Event listener that expects a given {@link TaskEvent} once in its {@link #onEvent(TaskEvent)}
     * call and will fail for any subsequent call.
     *
     * <p>Be sure to check that {@link #fired} is <tt>true</tt> to ensure that this handle has been
     * called once.
     */
    private static class OneShotEventListener implements EventListener<TaskEvent> {
        private final TaskEvent expected;
        boolean fired = false;

        OneShotEventListener(TaskEvent expected) {
            this.expected = expected;
        }

        public void onEvent(TaskEvent actual) {
            checkState(!fired, "Should only fire once");
            fired = true;
            checkArgument(
                    actual == expected,
                    "Fired on unexpected event: %s (expected: %s)",
                    actual,
                    expected);
        }
    }

    /**
     * Event listener which ensures that it's {@link #onEvent(TaskEvent)} method is never called.
     */
    private static class ZeroShotEventListener implements EventListener<TaskEvent> {
        public void onEvent(TaskEvent actual) {
            throw new IllegalStateException("Should never fire");
        }
    }
}

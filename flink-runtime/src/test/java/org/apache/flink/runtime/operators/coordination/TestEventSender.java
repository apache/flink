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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * A test implementation of the BiFunction interface used as the underlying event sender in the
 * {@link OperatorCoordinatorHolder}.
 */
final class TestEventSender
        implements BiFunction<
                SerializedValue<OperatorEvent>, Integer, CompletableFuture<Acknowledge>> {

    final ArrayList<EventWithSubtask> events = new ArrayList<>();

    @Nullable private final Throwable failureCause;

    /** Creates a sender that collects events and acknowledges all events successfully. */
    TestEventSender() {
        this(null);
    }

    /**
     * Creates a sender that collects events and fails all the send-futures with the given
     * exception, if it is non-null.
     */
    TestEventSender(@Nullable Throwable failureCause) {
        this.failureCause = failureCause;
    }

    @Override
    public CompletableFuture<Acknowledge> apply(
            SerializedValue<OperatorEvent> event, Integer subtask) {
        final OperatorEvent deserializedEvent;
        try {
            deserializedEvent = event.deserializeValue(getClass().getClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new AssertionError(e);
        }
        events.add(new EventWithSubtask(deserializedEvent, subtask));

        return failureCause == null
                ? CompletableFuture.completedFuture(Acknowledge.get())
                : FutureUtils.completedExceptionally(failureCause);
    }

    // ------------------------------------------------------------------------

    /** A combination of an {@link OperatorEvent} and the target subtask it is sent to. */
    static final class EventWithSubtask {

        final OperatorEvent event;
        final int subtask;

        EventWithSubtask(OperatorEvent event, int subtask) {
            this.event = event;
            this.subtask = subtask;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final EventWithSubtask that = (EventWithSubtask) o;
            return subtask == that.subtask && event.equals(that.event);
        }

        @Override
        public int hashCode() {
            return Objects.hash(event, subtask);
        }

        @Override
        public String toString() {
            return event + " => subtask " + subtask;
        }
    }
}

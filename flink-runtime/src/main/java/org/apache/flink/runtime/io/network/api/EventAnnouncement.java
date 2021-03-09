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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link EventAnnouncement} is announcing presence or receiving of an {@link AbstractEvent}. That
 * {@link #announcedEvent} is identified by it's sequence number.
 */
public class EventAnnouncement extends RuntimeEvent {

    private final AbstractEvent announcedEvent;
    private final int sequenceNumber;

    public EventAnnouncement(AbstractEvent announcedEvent, int sequenceNumber) {
        this.announcedEvent = announcedEvent;
        this.sequenceNumber = sequenceNumber;
    }

    public AbstractEvent getAnnouncedEvent() {
        return announcedEvent;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    //
    //  These methods are inherited form the generic serialization of AbstractEvent
    //  but would require the CheckpointBarrier to be mutable. Since all serialization
    //  for events goes through the EventSerializer class, which has special serialization
    //  for the CheckpointBarrier, we don't need these methods
    //

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hash(announcedEvent, sequenceNumber);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (other == null || other.getClass() != EventAnnouncement.class) {
            return false;
        } else {
            EventAnnouncement that = (EventAnnouncement) other;
            return Objects.equals(this.announcedEvent, that.announcedEvent)
                    && this.sequenceNumber == that.sequenceNumber;
        }
    }

    @Override
    public String toString() {
        return String.format("Announcement of [%s] at %d", announcedEvent, sequenceNumber);
    }
}

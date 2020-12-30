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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Either type for {@link Buffer} or {@link AbstractEvent} instances tagged with the channel index,
 * from which they were received.
 */
public class BufferOrEvent {

    private final Buffer buffer;

    private final AbstractEvent event;

    private final boolean hasPriority;

    /**
     * Indicate availability of further instances for the union input gate. This is not needed
     * outside of the input gate unioning logic and cannot be set outside of the consumer package.
     */
    private boolean moreAvailable;

    private final boolean morePriorityEvents;

    private InputChannelInfo channelInfo;

    private final int size;

    public BufferOrEvent(
            Buffer buffer,
            InputChannelInfo channelInfo,
            boolean moreAvailable,
            boolean morePriorityEvents) {
        this.buffer = checkNotNull(buffer);
        this.hasPriority = false;
        this.event = null;
        this.channelInfo = channelInfo;
        this.moreAvailable = moreAvailable;
        this.size = buffer.getSize();
        this.morePriorityEvents = morePriorityEvents;
    }

    public BufferOrEvent(
            AbstractEvent event,
            boolean hasPriority,
            InputChannelInfo channelInfo,
            boolean moreAvailable,
            int size,
            boolean morePriorityEvents) {
        this.buffer = null;
        this.hasPriority = hasPriority;
        this.event = checkNotNull(event);
        this.channelInfo = channelInfo;
        this.moreAvailable = moreAvailable;
        this.size = size;
        this.morePriorityEvents = morePriorityEvents;
    }

    @VisibleForTesting
    public BufferOrEvent(Buffer buffer, InputChannelInfo channelInfo) {
        this(buffer, channelInfo, true, false);
    }

    @VisibleForTesting
    public BufferOrEvent(AbstractEvent event, InputChannelInfo channelInfo) {
        this(event, false, channelInfo, true, 0, false);
    }

    public boolean isBuffer() {
        return buffer != null;
    }

    public boolean isEvent() {
        return event != null;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public AbstractEvent getEvent() {
        return event;
    }

    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    public void setChannelInfo(InputChannelInfo channelInfo) {
        this.channelInfo = channelInfo;
    }

    public boolean moreAvailable() {
        return moreAvailable;
    }

    public boolean morePriorityEvents() {
        return morePriorityEvents;
    }

    @Override
    public String toString() {
        return String.format(
                "BufferOrEvent [%s, channelInfo = %s, size = %d]",
                isBuffer() ? buffer : (event + " (prio=" + hasPriority + ")"), channelInfo, size);
    }

    public void setMoreAvailable(boolean moreAvailable) {
        this.moreAvailable = moreAvailable;
    }

    public int getSize() {
        return size;
    }

    public boolean hasPriority() {
        return hasPriority;
    }
}

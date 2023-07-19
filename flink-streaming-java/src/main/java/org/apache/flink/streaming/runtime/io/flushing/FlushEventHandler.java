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

package org.apache.flink.streaming.runtime.io.flushing;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.FlushEvent;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;

import java.io.Closeable;
import java.io.IOException;

/**
 * The {@link FlushEventHandler} keeps track of the latest flush event received by a task. It would
 * only broadcast flush events to down stream operators on receiving a flush event with greater id.
 */
public class FlushEventHandler implements Closeable {

    private final CheckpointableTask toNotifyOnFlushEvent;

    private final String taskName;

    /** The highest flush event ID encountered so far. */
    private long lastFlushingEventID = -1;

    private long localFlushEventIDCounter = -1L;

    public FlushEventHandler(CheckpointableTask toNotifyOnFlushEvent, String taskName) {
        this.toNotifyOnFlushEvent = toNotifyOnFlushEvent;
        this.taskName = taskName;
    }

    /**
     * Invoked when a task receives flush event from input channels.
     *
     * @param flushEvent The flush event from upstream tasks.
     * @param channelInfo of which the flush event is received.
     */
    public void processGlobalFlushEvent(FlushEvent flushEvent, InputChannelInfo channelInfo)
            throws IOException {
        if (flushEvent.getFlushEventId() > lastFlushingEventID) {
            lastFlushingEventID = flushEvent.getFlushEventId();
            toNotifyOnFlushEvent.triggerFlushEventOnEvent(flushEvent);
        }
    }

    public void processLocalFlushEvent() throws IOException {
        localFlushEventIDCounter++;
        toNotifyOnFlushEvent.triggerLocalFlushEvent(localFlushEventIDCounter);
    }

    @Override
    public void close() throws IOException {}
}

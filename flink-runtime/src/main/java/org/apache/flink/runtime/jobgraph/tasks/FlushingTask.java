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
package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.io.network.api.FlushEvent;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface FlushingTask {
    /**
     * This method is called when a task receives a flush event with higher id from one of the input
     * channels.
     *
     * @param flushEvent The flush event received from upstream tasks.
     */
    void triggerFlushEventOnEvent(FlushEvent flushEvent) throws IOException;

    void triggerLocalFlushEvent(long flushEventID) throws IOException;

    /**
     * This method is used to broadcast flush events to downstream operators, asynchronously by the
     * checkpoint coordinator.
     *
     * <p>This method is called for tasks that start the flushing operation by injecting the initial
     * flush events, i.e., the source tasks. In contrast, flush events on downstream tasks,
     * triggered by receiving flush events, invoke the {@link #triggerFlushEventOnEvent(FlushEvent)}
     * method.
     */
    CompletableFuture<Boolean> triggerFlushEventAsync(long flushEventID, long flushEventTimeStamp);
}

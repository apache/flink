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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.util.event.EventListener;

import org.apache.flink.shaded.guava30.com.google.common.collect.HashMultimap;
import org.apache.flink.shaded.guava30.com.google.common.collect.Multimap;

/**
 * The event handler manages {@link EventListener} instances and allows to to publish events to
 * them.
 */
public class TaskEventHandler {

    /** Listeners for each event type. */
    private final Multimap<Class<? extends TaskEvent>, EventListener<TaskEvent>> listeners =
            HashMultimap.create();

    public void subscribe(EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType) {
        synchronized (listeners) {
            listeners.put(eventType, listener);
        }
    }

    /**
     * Publishes the task event to all subscribed event listeners.
     *
     * @param event The event to publish.
     */
    public void publish(TaskEvent event) {
        synchronized (listeners) {
            for (EventListener<TaskEvent> listener : listeners.get(event.getClass())) {
                listener.onEvent(event);
            }
        }
    }
}

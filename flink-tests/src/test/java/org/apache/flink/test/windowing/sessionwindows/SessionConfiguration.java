/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.windowing.sessionwindows;

import org.apache.flink.util.Preconditions;

/**
 * Configuration data for a session.
 *
 * @param <K> type of session key
 * @param <E> type of session event
 */
public final class SessionConfiguration<K, E> {

    // key of the session
    private final K key;

    // id of the session w.r.t. key
    private final int sessionId;

    // allowed gap between events in one session
    private final long gap;

    // minimum event time among all events in the session
    private final long minEventTimestamp;

    // number of timely events in the session
    private final int numberOfTimelyEvents;

    // factory that produces the events for the session from metadata such as timestamps
    private final GeneratorEventFactory<K, E> eventFactory;

    public SessionConfiguration(
            K key,
            int sessionId,
            long gap,
            long minEventTimestamp,
            int numberOfTimelyEvents,
            GeneratorEventFactory<K, E> eventFactory) {

        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(eventFactory);
        Preconditions.checkArgument(numberOfTimelyEvents > 0);
        Preconditions.checkArgument(gap > 0);

        this.key = key;
        this.eventFactory = eventFactory;
        this.sessionId = sessionId;
        this.gap = gap;
        this.numberOfTimelyEvents = numberOfTimelyEvents;
        this.minEventTimestamp = minEventTimestamp;
    }

    public K getKey() {
        return key;
    }

    public GeneratorEventFactory<K, E> getEventFactory() {
        return eventFactory;
    }

    public long getGap() {
        return gap;
    }

    public long getMinEventTimestamp() {
        return minEventTimestamp;
    }

    public int getNumberOfTimelyEvents() {
        return numberOfTimelyEvents;
    }

    public int getSessionId() {
        return sessionId;
    }

    public static <K, E> SessionConfiguration<K, E> of(
            K key,
            int sessionId,
            long timeout,
            long startTimestamp,
            int numberOfEvents,
            GeneratorEventFactory<K, E> eventFactory) {
        return new SessionConfiguration<>(
                key, sessionId, timeout, startTimestamp, numberOfEvents, eventFactory);
    }

    @Override
    public String toString() {
        return "SessionConfiguration{"
                + "key="
                + key
                + ", sessionId="
                + sessionId
                + ", gap="
                + gap
                + ", minEventTimestamp="
                + minEventTimestamp
                + ", numberOfTimelyEvents="
                + numberOfTimelyEvents
                + ", eventFactory="
                + eventFactory
                + '}';
    }

    public SessionConfiguration<K, E> getFollowupSessionConfiguration(long startTimestamp) {
        return SessionConfiguration.of(
                getKey(),
                getSessionId() + 1,
                getGap(),
                startTimestamp,
                getNumberOfTimelyEvents(),
                getEventFactory());
    }
}

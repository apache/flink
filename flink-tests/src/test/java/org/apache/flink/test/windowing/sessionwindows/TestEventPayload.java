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

/** Test payload that contains useful information for the correctness checks in our test. */
public final class TestEventPayload {

    // id of the session w.r.t the key
    private int sessionId;

    // id of the event w.r.t. tje session id
    private int eventId;

    // the watermark under which the event was emitted
    private long watermark;

    // the timing characteristic of the event w.r.t. the watermark
    private SessionEventGeneratorImpl.Timing timing;

    public TestEventPayload(
            long watermark,
            int sessionId,
            int eventSequenceNumber,
            SessionEventGeneratorImpl.Timing timing) {
        setWatermark(watermark);
        setSessionId(sessionId);
        setEventId(eventSequenceNumber);
        setTiming(timing);
    }

    /** @return global watermark at the time this event was generated */
    public long getWatermark() {
        return watermark;
    }

    public void setWatermark(long watermark) {
        this.watermark = watermark;
    }

    /**
     * @return id of the session to identify a sessions in the sequence of all sessions for the same
     *     key
     */
    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    /** @return a sequence number that acts as an id for the even inside the session */
    public int getEventId() {
        return eventId;
    }

    public void setEventId(int eventId) {
        this.eventId = eventId;
    }

    /**
     * @return indicates whether the event is on time, late within the timing, or late after the
     *     timing
     */
    public SessionEventGeneratorImpl.Timing getTiming() {
        return timing;
    }

    public void setTiming(SessionEventGeneratorImpl.Timing timing) {
        Preconditions.checkNotNull(timing);
        this.timing = timing;
    }

    @Override
    public String toString() {
        return "TestEventPayload{"
                + "sessionId="
                + sessionId
                + ", eventId="
                + eventId
                + ", watermark="
                + watermark
                + ", timing="
                + timing
                + '}';
    }

    public static TestEventPayload of(
            long watermark, int sessionId, int eventId, SessionEventGeneratorImpl.Timing timing) {
        return new TestEventPayload(watermark, sessionId, eventId, timing);
    }
}

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

package org.apache.flink.cep.dsl.model;

import java.io.Serializable;
import java.util.Objects;

/** User activity event model for DSL testing. */
public class UserActivityEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String userId;
    private String eventType;
    private String page;
    private long duration;
    private long timestamp;
    private String sessionId;
    private int count;

    public UserActivityEvent() {}

    public UserActivityEvent(
            String userId,
            String eventType,
            String page,
            long duration,
            long timestamp,
            String sessionId,
            int count) {
        this.userId = userId;
        this.eventType = eventType;
        this.page = page;
        this.duration = duration;
        this.timestamp = timestamp;
        this.sessionId = sessionId;
        this.count = count;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserActivityEvent that = (UserActivityEvent) o;
        return duration == that.duration
                && timestamp == that.timestamp
                && count == that.count
                && Objects.equals(userId, that.userId)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(page, that.page)
                && Objects.equals(sessionId, that.sessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, eventType, page, duration, timestamp, sessionId, count);
    }

    @Override
    public String toString() {
        return "UserActivityEvent{"
                + "userId='"
                + userId
                + '\''
                + ", eventType='"
                + eventType
                + '\''
                + ", page='"
                + page
                + '\''
                + ", duration="
                + duration
                + ", timestamp="
                + timestamp
                + ", sessionId='"
                + sessionId
                + '\''
                + ", count="
                + count
                + '}';
    }
}

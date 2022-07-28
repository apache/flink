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

package org.apache.flink.connector.pulsar.source.enumerator.cursor.stop;

import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;

import org.apache.pulsar.client.api.Message;

/** Stop consuming message at the given event time. */
public class EventTimestampStopCursor implements StopCursor {
    private static final long serialVersionUID = 2391576769339369027L;

    private final long timestamp;
    private final boolean inclusive;

    public EventTimestampStopCursor(long timestamp, boolean inclusive) {
        this.timestamp = timestamp;
        this.inclusive = inclusive;
    }

    @Override
    public StopCondition shouldStop(Message<?> message) {
        long eventTime = message.getEventTime();
        return StopCondition.compare(timestamp, eventTime, inclusive);
    }
}

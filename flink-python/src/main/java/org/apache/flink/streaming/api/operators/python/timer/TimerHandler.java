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

package org.apache.flink.streaming.api.operators.python.timer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.types.Row;

/** Utilities to handle triggered timer. */
@Internal
public final class TimerHandler {

    /** Reusable row for timer data. */
    private final Row reusableTimerData;

    public TimerHandler() {
        this.reusableTimerData = new Row(5);
    }

    public Row buildTimerData(
            TimeDomain timeDomain,
            long watermark,
            long timestamp,
            Row key,
            byte[] encodedNamespace) {
        if (timeDomain == TimeDomain.PROCESSING_TIME) {
            reusableTimerData.setField(0, TimerType.PROCESSING_TIME.value);
        } else {
            reusableTimerData.setField(0, TimerType.EVENT_TIME.value);
        }
        reusableTimerData.setField(1, watermark);
        reusableTimerData.setField(2, timestamp);
        reusableTimerData.setField(3, key);
        reusableTimerData.setField(4, encodedNamespace);
        return reusableTimerData;
    }

    /** The type of the timer. */
    private enum TimerType {
        EVENT_TIME((byte) 0),
        PROCESSING_TIME((byte) 1);

        public final byte value;

        TimerType(byte value) {
            this.value = value;
        }
    }
}

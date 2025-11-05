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

package org.apache.flink.table.runtime.sequencedmultisetstate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.util.clock.SystemClock;

import java.io.Serializable;

@Internal
@FunctionalInterface
public interface TimeSelector extends Serializable {

    long getTimestamp(long elementTimestamp);

    static TimeSelector getTimeDomain(TimeDomain timeDomain) {
        switch (timeDomain) {
            case EVENT_TIME:
                return elementTimestamp -> elementTimestamp;
            case PROCESSING_TIME:
                return elementTimestamp -> SystemClock.getInstance().absoluteTimeMillis();
            default:
                throw new IllegalStateException("unknown time domain: " + timeDomain);
        }
    }
}

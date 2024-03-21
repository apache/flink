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

package org.apache.flink.process.impl.common;

import org.apache.flink.process.api.common.Collector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Optional;

/** The base {@link Collector} which take care of records timestamp. */
public abstract class TimestampCollector<OUT> implements Collector<OUT> {
    protected final StreamRecord<OUT> reuse = new StreamRecord<>(null);

    public void setTimestamp(StreamRecord<?> timestampBase) {
        if (timestampBase.hasTimestamp()) {
            setAbsoluteTimestamp(timestampBase.getTimestamp());
        } else {
            eraseTimestamp();
        }
    }

    public void setAbsoluteTimestamp(long timestamp) {
        reuse.setTimestamp(timestamp);
    }

    public void eraseTimestamp() {
        reuse.eraseTimestamp();
    }

    /** Get the timestamp of the latest seen record. */
    public Optional<Long> getLatestTimestamp() {
        if (reuse.hasTimestamp()) {
            return Optional.of(reuse.getTimestamp());
        } else {
            return Optional.empty();
        }
    }
}

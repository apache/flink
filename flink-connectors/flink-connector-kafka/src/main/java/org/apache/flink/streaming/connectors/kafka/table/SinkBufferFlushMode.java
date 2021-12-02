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

package org.apache.flink.streaming.connectors.kafka.table;

import java.io.Serializable;
import java.util.Objects;

/** Sink buffer flush configuration. */
public class SinkBufferFlushMode implements Serializable {

    private static final int DISABLED_BATCH_SIZE = 0;
    private static final long DISABLED_BATCH_INTERVAL = 0L;

    public static final SinkBufferFlushMode DISABLED =
            new SinkBufferFlushMode(DISABLED_BATCH_SIZE, DISABLED_BATCH_INTERVAL);

    private final int batchSize;
    private final long batchIntervalMs;

    public SinkBufferFlushMode(int batchSize, long batchIntervalMs) {
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;

        // validation
        if (isEnabled()
                && !(batchSize > DISABLED_BATCH_SIZE
                        && batchIntervalMs > DISABLED_BATCH_INTERVAL)) {
            throw new IllegalArgumentException(
                    String.format(
                            "batchSize and batchInterval must greater than zero if buffer flush is enabled,"
                                    + " but got batchSize=%s and batchIntervalMs=%s",
                            batchSize, batchIntervalMs));
        }
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public boolean isEnabled() {
        return !(batchSize == DISABLED_BATCH_SIZE && batchIntervalMs == DISABLED_BATCH_INTERVAL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SinkBufferFlushMode that = (SinkBufferFlushMode) o;
        return batchSize == that.batchSize && batchIntervalMs == that.batchIntervalMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchSize, batchIntervalMs);
    }
}

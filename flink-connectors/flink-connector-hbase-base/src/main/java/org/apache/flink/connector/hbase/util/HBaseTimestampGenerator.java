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

package org.apache.flink.connector.hbase.util;

/** Generate timestamp for HBase mutation. */
public abstract class HBaseTimestampGenerator {
    private static final long START_SYSTEM_TIME_NANO = System.currentTimeMillis() * 1_000_000L;
    private static final long START_JVM_TIME_NANO = System.nanoTime();

    public abstract long get();

    protected long getCurrentSystemTimeNano() {
        return START_SYSTEM_TIME_NANO + (System.nanoTime() - START_JVM_TIME_NANO);
    }

    public static HBaseTimestampGenerator stronglyIncreasing() {
        return new StronglyIncreasingGenerator();
    }

    /** Generator strongly increasing timestamp in nanosecond. */
    static class StronglyIncreasingGenerator extends HBaseTimestampGenerator {
        private long currentSystemTimeNano;

        StronglyIncreasingGenerator() {
            currentSystemTimeNano = getCurrentSystemTimeNano();
        }

        @Override
        public long get() {
            long nowNano = getCurrentSystemTimeNano();
            if (nowNano <= currentSystemTimeNano) {
                nowNano = currentSystemTimeNano + 1;
            }
            currentSystemTimeNano = nowNano;
            return nowNano;
        }
    }
}

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

package org.apache.flink.runtime.state.ttl;

import javax.annotation.Nullable;

/** Common functions related to State TTL. */
public class TtlUtils {
    static <V> boolean expired(
            @Nullable TtlValue<V> ttlValue, long ttl, TtlTimeProvider timeProvider) {
        return expired(ttlValue, ttl, timeProvider.currentTimestamp());
    }

    static <V> boolean expired(@Nullable TtlValue<V> ttlValue, long ttl, long currentTimestamp) {
        return ttlValue != null
                && expired(ttlValue.getLastAccessTimestamp(), ttl, currentTimestamp);
    }

    static boolean expired(long ts, long ttl, TtlTimeProvider timeProvider) {
        return expired(ts, ttl, timeProvider.currentTimestamp());
    }

    public static boolean expired(long ts, long ttl, long currentTimestamp) {
        return getExpirationTimestamp(ts, ttl) <= currentTimestamp;
    }

    private static long getExpirationTimestamp(long ts, long ttl) {
        long ttlWithoutOverflow = ts > 0 ? Math.min(Long.MAX_VALUE - ts, ttl) : ttl;
        return ts + ttlWithoutOverflow;
    }

    static <V> TtlValue<V> wrapWithTs(V value, long ts) {
        return new TtlValue<>(value, ts);
    }
}

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

package org.apache.flink.connector.cassandra.source.split;

import java.util.Set;

/** Mutable {@link CassandraSplit} for state management. */
public class CassandraSplitState {
    private final Set<RingRange> unprocessedRingRanges;
    private final String splitId;

    public CassandraSplitState(Set<RingRange> unprocessedRingRanges, String splitId) {
        this.unprocessedRingRanges = unprocessedRingRanges;
        this.splitId = splitId;
    }

    public Set<RingRange> getUnprocessedRingRanges() {
        return unprocessedRingRanges;
    }

    public void markRingRangeAsFinished(RingRange ringRange) {
        unprocessedRingRanges.remove(ringRange);
    }

    public void markAllRingRangesAsFinished() {
        unprocessedRingRanges.clear();
    }

    public boolean isEmpty() {
        return unprocessedRingRanges.isEmpty();
    }

    public String getSplitId() {
        return splitId;
    }
}

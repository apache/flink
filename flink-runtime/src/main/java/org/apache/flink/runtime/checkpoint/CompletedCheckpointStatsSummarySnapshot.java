/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Immutable snapshot of {@link CompletedCheckpointStatsSummary}. */
@Internal
public class CompletedCheckpointStatsSummarySnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final StatsSummarySnapshot duration;
    private final StatsSummarySnapshot persistedData;
    private final StatsSummarySnapshot processedData;
    private final StatsSummarySnapshot stateSize;

    public CompletedCheckpointStatsSummarySnapshot(
            StatsSummarySnapshot duration,
            StatsSummarySnapshot processedData,
            StatsSummarySnapshot persistedData,
            StatsSummarySnapshot stateSize) {
        this.duration = checkNotNull(duration);
        this.persistedData = checkNotNull(persistedData);
        this.processedData = checkNotNull(processedData);
        this.stateSize = checkNotNull(stateSize);
    }

    public static CompletedCheckpointStatsSummarySnapshot empty() {
        return new CompletedCheckpointStatsSummarySnapshot(
                StatsSummarySnapshot.empty(),
                StatsSummarySnapshot.empty(),
                StatsSummarySnapshot.empty(),
                StatsSummarySnapshot.empty());
    }

    public StatsSummarySnapshot getEndToEndDurationStats() {
        return duration;
    }

    public StatsSummarySnapshot getPersistedDataStats() {
        return persistedData;
    }

    public StatsSummarySnapshot getProcessedDataStats() {
        return processedData;
    }

    public StatsSummarySnapshot getStateSizeStats() {
        return stateSize;
    }
}

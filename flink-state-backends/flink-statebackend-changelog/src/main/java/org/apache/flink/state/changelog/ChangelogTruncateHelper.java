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

package org.apache.flink.state.changelog;

import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A helper class to track changelog usage by {@link ChangelogKeyedStateBackend}. A changelog
 * segment is not used when:
 *
 * <ol>
 *   <li>All the checkpoints using it were subsumed, and
 *   <li>Is was materialized (and therefore will not be used in future checkpoints)
 * </ol>
 *
 * <p>At this point, {@link org.apache.flink.runtime.state.changelog.StateChangelogWriter} is
 * notified and will {@link org.apache.flink.runtime.state.changelog.StateChangelogWriter#truncate
 * truncate} the changelog. That implies discarding any (changelog) state that is not known to JM.
 */
class ChangelogTruncateHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ChangelogTruncateHelper.class);

    private final StateChangelogWriter<?> stateChangelogWriter;
    private final NavigableMap<Long, SequenceNumber> checkpointedUpTo = new TreeMap<>();

    private SequenceNumber subsumedUpTo;

    private SequenceNumber materializedUpTo;

    ChangelogTruncateHelper(StateChangelogWriter<?> stateChangelogWriter) {
        this.stateChangelogWriter = stateChangelogWriter;
    }

    /**
     * Set the highest {@link SequenceNumber} of changelog used by the given checkpoint.
     *
     * @param lastUploadedTo exclusive
     */
    public void checkpoint(long checkpointId, SequenceNumber lastUploadedTo) {
        checkpointedUpTo.put(checkpointId, lastUploadedTo);
    }

    /** Handle checkpoint subsumption, potentially {@link #truncate() truncating} the changelog. */
    public void checkpointSubsumed(long checkpointId) {
        SequenceNumber sqn = checkpointedUpTo.get(checkpointId);
        LOG.debug("checkpoint {} subsumed, max sqn: {}", checkpointId, sqn);
        if (sqn != null) {
            subsumedUpTo = sqn;
            checkpointedUpTo.headMap(checkpointId, true).clear();
            truncate();
        }
    }

    /**
     * Handle changelog materialization, potentially {@link #truncate() truncating} the changelog.
     *
     * @param upTo exclusive
     */
    public void materialized(SequenceNumber upTo) {
        materializedUpTo = upTo;
        truncate();
    }

    private void truncate() {
        if (subsumedUpTo != null && materializedUpTo != null) {
            SequenceNumber to =
                    subsumedUpTo.compareTo(materializedUpTo) < 0 ? subsumedUpTo : materializedUpTo;
            LOG.debug(
                    "truncate changelog to {} (subsumed up to: {}, materialized up to: {})",
                    to,
                    subsumedUpTo,
                    materializedUpTo);
            stateChangelogWriter.truncate(to);
        }
    }
}

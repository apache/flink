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

package org.apache.flink.changelog.fs;

import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.UUID;

import static java.util.Collections.unmodifiableList;

/**
 * A set of changes made to some state(s) by a single state backend during a single checkpoint.
 * There can be zero or more change sets for a single checkpoint. Thread-safe with the assumption
 * that constructor arguments are not modified outside.
 */
@ThreadSafe
class StateChangeSet {
    private final UUID logId;
    private final List<StateChange> changes;
    private final SequenceNumber sequenceNumber;

    public StateChangeSet(UUID logId, SequenceNumber sequenceNumber, List<StateChange> changes) {
        this.logId = logId;
        this.changes = unmodifiableList(changes);
        this.sequenceNumber = sequenceNumber;
    }

    public UUID getLogId() {
        return logId;
    }

    public SequenceNumber getSequenceNumber() {
        return sequenceNumber;
    }

    public List<StateChange> getChanges() {
        return changes;
    }

    public long getSize() {
        long size = 0;
        for (StateChange change : changes) {
            size += change.getChange().length;
        }
        return size;
    }

    @Override
    public String toString() {
        return "logId=" + logId + ", sequenceNumber=" + sequenceNumber + ", changes=" + changes;
    }
}

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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A checkpoint of the current state of the containing the currently pending splits that are not yet
 * assigned.
 */
@PublicEvolving
public class PendingSplitsCheckpoint<SplitT extends FileSourceSplit> {

    /** The splits in the checkpoint. */
    private final Collection<SplitT> splits;

    /**
     * The splits that are no longer in the enumerator checkpoint, but have been processed before
     * and should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final Collection<String> alreadyProcessedSplits;

    /**
     * The cached byte representation from the last serialization step. This helps to avoid paying
     * repeated serialization cost for the same checkpoint object. This field is used by {@link
     * PendingSplitsCheckpointSerializer}.
     */
    @Nullable byte[] serializedFormCache;

    protected PendingSplitsCheckpoint(
            Collection<SplitT> splits, Collection<String> alreadyProcessedSplits) {
        this.splits = Collections.unmodifiableCollection(splits);
        this.alreadyProcessedSplits = Collections.unmodifiableCollection(alreadyProcessedSplits);
    }

    // ------------------------------------------------------------------------

    public Collection<SplitT> getSplits() {
        return splits;
    }

    public Collection<String> getAlreadyProcessedSplits() {
        return alreadyProcessedSplits;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "PendingSplitsCheckpoint:\n"
                + "\t\t Pending Splits: "
                + splits
                + '\n'
                + "\t\t Processed Paths: "
                + alreadyProcessedSplits
                + '\n';
    }

    // ------------------------------------------------------------------------
    //  factories
    // ------------------------------------------------------------------------

    public static <T extends FileSourceSplit> PendingSplitsCheckpoint<T> fromCollectionSnapshot(
            final Collection<T> splits) {
        checkNotNull(splits);

        // create a copy of the collection to make sure this checkpoint is immutable
        final Collection<T> copy = new ArrayList<>(splits);
        return new PendingSplitsCheckpoint<>(copy, Collections.emptySet());
    }

    public static <T extends FileSourceSplit> PendingSplitsCheckpoint<T> fromCollectionSnapshot(
            final Collection<T> splits, final Collection<String> alreadyProcessedSplits) {
        checkNotNull(splits);

        // create a copy of the collection to make sure this checkpoint is immutable
        final Collection<T> splitsCopy = new ArrayList<>(splits);
        final Collection<String> processedSplitsCopy = new ArrayList<>(alreadyProcessedSplits);

        return new PendingSplitsCheckpoint<>(splitsCopy, processedSplitsCopy);
    }

    static <T extends FileSourceSplit> PendingSplitsCheckpoint<T> reusingCollection(
            final Collection<T> splits, final Collection<String> alreadyProcessedSplits) {
        return new PendingSplitsCheckpoint<>(splits, alreadyProcessedSplits);
    }
}

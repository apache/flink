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

package org.apache.flink.connector.file.src.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * The {@code FileSplitAssigner} is responsible for deciding what split should be processed next by
 * which node. It determines split processing order and locality.
 */
@PublicEvolving
public interface FileSplitAssigner {

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     *
     * @param subTaskId the subtask id of the source reader node.
     * @param hostname the host name of the source reader node.
     */
    Optional<FileSourceSplit> getNext(int subTaskId, @Nullable String hostname);

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added, or when new splits got discovered.
     */
    void addSplits(Collection<FileSourceSplit> splits);

    /** Gets the remaining splits that this assigner has pending. */
    Collection<FileSourceSplit> remainingSplits();

    // ------------------------------------------------------------------------

    /**
     * Factory for the {@code FileSplitAssigner}, to allow the {@code FileSplitAssigner} to be
     * eagerly initialized and to not be serializable.
     */
    @FunctionalInterface
    interface Provider extends Serializable {

        /**
         * Creates a new {@code FileSplitAssigner} that starts with the given set of initial splits.
         */
        FileSplitAssigner create(Collection<FileSourceSplit> initialSplits);
    }
}

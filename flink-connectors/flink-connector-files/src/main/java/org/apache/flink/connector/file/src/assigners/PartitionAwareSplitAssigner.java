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
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link FileSplitAssigner} that assigns splits in a partitioned way. Splits belonging to
 * specific partition can be consumed only by one source task
 */
@PublicEvolving
public class PartitionAwareSplitAssigner implements FileSplitAssigner {

    private final Map<Integer, List<FileSourceSplit>> partitionedSplits;
    private final List<Path> partitionPaths;

    private Map<Integer, Set<Integer>> subtaskToPartitionAssignment;

    public PartitionAwareSplitAssigner(
            Collection<FileSourceSplit> splits, List<Path> partitionPaths) {
        this.partitionPaths = partitionPaths;
        partitionedSplits =
                splits.stream()
                        .collect(
                                Collectors.groupingBy(
                                        split -> deriveParentIdx(split.path(), partitionPaths)));
    }

    private int deriveParentIdx(Path path, List<Path> possibleParents) {
        for (int i = 0; i < possibleParents.size(); ++i) {
            if (isParentPath(possibleParents.get(i), path)) {
                return i;
            }
        }
        throw new RuntimeException("Unexpected path error happened");
    }

    private boolean isParentPath(Path maybeParent, Path path) {
        Path pathParent = path.getParent();
        while (pathParent != null) {
            if (pathParent.getPath().equals(maybeParent.getPath())) {
                return true;
            }
            pathParent = pathParent.getParent();
        }
        return false;
    }

    private void initializePartitionAssignment(int registeredTasks) {
        this.subtaskToPartitionAssignment = new HashMap<>();
        int length = partitionedSplits.size();
        if (length < registeredTasks) {
            throw new IllegalArgumentException(
                    "Number of pieces cannot be greater than the length of the list");
        }

        int quotient = length / registeredTasks;
        int remainder = length % registeredTasks;

        //        List<List<Integer>> pieces = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < registeredTasks; i++) {
            int end = start + quotient + (i < remainder ? 1 : 0);
            Set<Integer> partitions = new HashSet<>();
            for (int p = start; p < end; ++p) {
                partitions.add(p);
            }
            subtaskToPartitionAssignment.put(i, partitions);
            start = end;
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public Optional<FileSourceSplit> getNext(String hostname) {
        throw new RuntimeException(
                "PartitionAwareSplitAssigner only supports getNext(@Nullable String hostname, int subtaskID)");
    }

    @Override
    public Optional<FileSourceSplit> getNext(
            @Nullable String hostname, int subtaskID, int registeredTasks) {
        if (subtaskToPartitionAssignment == null || subtaskToPartitionAssignment.isEmpty()) {
            initializePartitionAssignment(registeredTasks);
        }
        Set<Integer> partitions =
                subtaskToPartitionAssignment.getOrDefault(subtaskID, Collections.emptySet());
        for (Integer p : partitions) {
            List<FileSourceSplit> partition =
                    partitionedSplits.getOrDefault(p, Collections.emptyList());
            final int size = partition.size();
            if (size != 0) {
                return Optional.of(partition.remove(size - 1));
            }
        }
        return Optional.empty();
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> newSplits) {
        for (FileSourceSplit split : newSplits) {
            final int idx = deriveParentIdx(split.path(), partitionPaths);
            partitionedSplits.computeIfAbsent(idx, v -> new ArrayList<>()).add(split);
        }
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        List<FileSourceSplit> remainingSplits = new ArrayList<>();
        partitionedSplits.values().forEach(remainingSplits::addAll);
        return remainingSplits;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "PartitionAwareSplitAssigner " + partitionedSplits;
    }
}

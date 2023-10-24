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

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/** Assign every split to fixed task id to reduce the failover cost in batch source. */
public class FixedFileSplitAssigner implements FileSplitAssigner {
    private static final int FILE_OPEN_COST = 4 * 1024 * 1024;

    private final Set<FileSourceSplit> unsignedSplits = new HashSet<>();
    private ArrayList<FileSourceSplit>[] fileSourceSplitsList;
    private final Map<String, Integer> splitIdToTaskId = new HashMap<>();

    public FixedFileSplitAssigner(Collection<FileSourceSplit> splits) {
        this.unsignedSplits.addAll(splits);
    }

    @SuppressWarnings("unchecked")
    public void setTaskParallelism(int parallelism) {
        Preconditions.checkState(parallelism > 0);
        fileSourceSplitsList = new ArrayList[parallelism];
        PriorityQueue<TaskAssignInfo> queue = new PriorityQueue<>();
        for (int i = 0; i < parallelism; i++) {
            fileSourceSplitsList[i] = new ArrayList<>();
            queue.add(new TaskAssignInfo(i, 0));
        }
        List<FileSourceSplit> sortedSplits =
                unsignedSplits.stream()
                        .sorted(Comparator.comparing(f -> -f.fileSize()))
                        .collect(Collectors.toList());
        for (FileSourceSplit split : sortedSplits) {
            TaskAssignInfo taskAssignInfo = queue.remove();
            splitIdToTaskId.put(split.splitId(), taskAssignInfo.subTask);
            fileSourceSplitsList[taskAssignInfo.subTask].add(split);
            taskAssignInfo.totalSize += FILE_OPEN_COST + split.length();
            queue.add(taskAssignInfo);
        }
    }

    @Override
    public Optional<FileSourceSplit> getNext(@Nullable String hostname, Integer subTask) {
        Preconditions.checkNotNull(subTask);
        List<FileSourceSplit> fileSourceSplits = fileSourceSplitsList[subTask];
        int size = fileSourceSplits.size();
        Optional<FileSourceSplit> splitOptional =
                size == 0 ? Optional.empty() : Optional.of(fileSourceSplits.remove(size - 1));
        splitOptional.ifPresent(unsignedSplits::remove);
        return splitOptional;
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> splits) {
        for (FileSourceSplit split : splits) {
            Integer taskId = splitIdToTaskId.get(split.splitId());
            // It should be used in non-continuous mode, so it can't happen.
            Preconditions.checkNotNull(
                    taskId, "The returned split not exits, " + "it can't happen!");
            fileSourceSplitsList[taskId].add(split);
            unsignedSplits.add(split);
        }
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        return Collections.unmodifiableCollection(unsignedSplits);
    }

    private static final class TaskAssignInfo implements Comparable<TaskAssignInfo> {
        private final int subTask;
        private long totalSize;

        public TaskAssignInfo(int subTask, long totalSize) {
            this.totalSize = totalSize;
            this.subTask = subTask;
        }

        @Override
        public int compareTo(FixedFileSplitAssigner.TaskAssignInfo o) {
            if (o.totalSize == totalSize) {
                return Integer.compare(subTask, o.subTask);
            }
            return Long.compare(totalSize, o.totalSize);
        }
    }
}

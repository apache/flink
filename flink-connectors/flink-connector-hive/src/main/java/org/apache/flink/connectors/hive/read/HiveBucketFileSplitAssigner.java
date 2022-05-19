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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;

import org.apache.hadoop.hive.ql.exec.Utilities;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A split assigner for Hive's bucket files. All the files belonged to same bucket will be assigned
 * to same reader.
 *
 * <p>NOTE: currently, the assigner can't guarantee such assignment in stream mode for the assigner
 * can't restore the previous assignment of bucket files after fail over.
 */
public class HiveBucketFileSplitAssigner implements FileSplitAssigner {

    // mapping from bucket id to splits
    private final Map<Integer, List<FileSourceSplit>> remainingBucketSplits = new HashMap<>();
    // mapping from reader to bucket id
    private final Map<Integer, Integer> assignments = new HashMap<>();

    public HiveBucketFileSplitAssigner(Collection<FileSourceSplit> splits) {
        addSplits(splits);
    }

    @Override
    public Optional<FileSourceSplit> getNext(int subTaskId, @Nullable String hostname) {
        // we have assigned one split of a bucket to the reader, still assign the splits with same
        // bucket id to it
        if (assignments.get(subTaskId) != null) {
            List<FileSourceSplit> splits = remainingBucketSplits.get(assignments.get(subTaskId));
            // there isn't any split for the bucket, try to assign it a new bucket
            if (splits.isEmpty()) {
                return assignNewBucketSplit(subTaskId);
            }
            // return a split belonged to the bucket
            return Optional.of(splits.remove(splits.size() - 1));
        } else {
            return assignNewBucketSplit(subTaskId);
        }
    }

    private Optional<FileSourceSplit> assignNewBucketSplit(int subTask) {
        // find any one bucket that there isn't any reader is reading it, and then assign it a split
        // belonged to the bucket
        for (Map.Entry<Integer, List<FileSourceSplit>> bucketSplitEntry :
                remainingBucketSplits.entrySet()) {
            int bucketId = bucketSplitEntry.getKey();
            // there isn't any reader is reading the bucket
            if (!assignments.containsValue(bucketId)) {
                List<FileSourceSplit> splits = bucketSplitEntry.getValue();
                // if there remains splits for the bucket, assign the split to the reader,
                // and update the assignment for the bucket
                if (splits.size() > 0) {
                    FileSourceSplit split = splits.remove(splits.size() - 1);
                    assignments.put(subTask, bucketId);
                    return Optional.of(split);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> splits) {
        for (FileSourceSplit split : splits) {
            Integer bucketId = Utilities.getBucketIdFromFile(split.path().getName());
            remainingBucketSplits.putIfAbsent(bucketId, new ArrayList<>());
            remainingBucketSplits.get(bucketId).add(split);
        }
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        Set<FileSourceSplit> remainingSplits = new HashSet<>();
        remainingBucketSplits.forEach((bucketId, splits) -> remainingSplits.addAll(splits));
        return remainingSplits;
    }
}

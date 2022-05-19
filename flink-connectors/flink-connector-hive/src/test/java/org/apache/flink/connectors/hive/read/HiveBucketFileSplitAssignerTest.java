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
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link HiveBucketFileSplitAssigner}. */
public class HiveBucketFileSplitAssignerTest {

    @Test
    public void testBucketFileSplitAssignment() {
        // case1: 2 reader, 2 buckets, each bucket has 2 files,
        List<FileSourceSplit> splits = createSplits(2, 2);
        FileSplitAssigner assigner = new HiveBucketFileSplitAssigner(splits);
        // reader0 should read bucket_0
        verifySplitBucketId(assigner.getNext(0, null), 0);
        // reader1 should read bucket_1
        verifySplitBucketId(assigner.getNext(1, null), 1);
        // reader1 should still read bucket_1
        verifySplitBucketId(assigner.getNext(1, null), 1);
        // reader1 should read nothing
        assertThat(assigner.getNext(1, null).isPresent()).isFalse();
        // reader0 should read bucket_0
        verifySplitBucketId(assigner.getNext(0, null), 0);
        // reader0 should read nothing
        assertThat(assigner.getNext(0, null).isPresent()).isFalse();

        // case2: 1 reader, 2 buckets, each bucket has 1 files
        splits = createSplits(2, 1);
        assigner = new HiveBucketFileSplitAssigner(splits);
        // reader0 should read bucket_0
        verifySplitBucketId(assigner.getNext(0, null), 0);
        // now reader0 should move to next bucket, that's bucket_1
        verifySplitBucketId(assigner.getNext(0, null), 1);
        // reader0 should read nothing
        assertThat(assigner.getNext(0, null).isPresent()).isFalse();

        // case3: 2 reader, 1 bucket, each bucket has 2 file
        splits = createSplits(1, 2);
        assigner = new HiveBucketFileSplitAssigner(splits);
        // reader0 should read bucket_0
        verifySplitBucketId(assigner.getNext(0, null), 0);
        // reader1 should read nothing
        assertThat(assigner.getNext(1, null).isPresent()).isFalse();
        // reader0 should read bucket_0
        verifySplitBucketId(assigner.getNext(0, null), 0);
        // reader0 should nothing
        assertThat(assigner.getNext(0, null).isPresent()).isFalse();
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private static List<FileSourceSplit> createSplits(int bucketNum, int filesOfBucket) {
        List<FileSourceSplit> splits = new ArrayList<>();
        for (int bucket = 0; bucket < bucketNum; bucket++) {
            for (int i = 0; i < filesOfBucket; i++) {
                String splitId = String.valueOf(bucket * bucketNum + i);
                String fileName = String.format("%05d_0_%s", bucket, splitId);
                splits.add(
                        new FileSourceSplit(
                                splitId, new Path(fileName), 0, 1024, 0, 1024, new String[0]));
            }
        }
        return splits;
    }

    private static void verifySplitBucketId(Optional<FileSourceSplit> split, int expectedBucketId) {
        assertThat(split.isPresent()).isTrue();
        int actualBucketId = Utilities.getBucketIdFromFile(split.get().path().getName());
        assertThat(actualBucketId).isEqualTo(expectedBucketId);
    }
}

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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions.PartitionCommitTriggerType;

import java.util.List;

import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_TRIGGER;

/**
 * Partition commit predicate. See {@link PartitionTimeCommitPredicate}. See {@link
 * ProcTimeCommitPredicate}
 */
public interface PartitionCommitPredicate {

    boolean isPartitionCommittable(PredicateContext predicateContext);

    /**
     * Context that {@link PartitionCommitPredicate} can use for getting context about a partition.
     */
    interface PredicateContext {
        /** Return the partition. */
        String partition();

        /** Return the creation time of the partition. */
        long createProcTime();

        /** Return the current process time. */
        long currentProcTime();

        /** Returns the current event-time watermark. */
        long currentWatermark();
    }

    static PredicateContext createPredicateContext(
            String partition, long createProcTime, long currentProcTime, long currentWatermark) {
        return new PredicateContext() {
            @Override
            public String partition() {
                return partition;
            }

            @Override
            public long createProcTime() {
                return createProcTime;
            }

            @Override
            public long currentProcTime() {
                return currentProcTime;
            }

            @Override
            public long currentWatermark() {
                return currentWatermark;
            }
        };
    }

    static PartitionCommitPredicate createPartitionTimeCommitPredicate(
            Configuration conf, ClassLoader cl, List<String> partitionKeys) {
        return new PartitionTimeCommitPredicate(conf, cl, partitionKeys);
    }

    static PartitionCommitPredicate createProcTimeCommitPredicate(Configuration conf) {
        return new ProcTimeCommitPredicate(conf);
    }

    static PartitionCommitPredicate create(
            Configuration conf, ClassLoader cl, List<String> partitionKeys) {
        PartitionCommitTriggerType trigger = conf.get(SINK_PARTITION_COMMIT_TRIGGER);
        switch (trigger) {
            case PARTITION_TIME:
                return createPartitionTimeCommitPredicate(conf, cl, partitionKeys);
            case PROCESS_TIME:
                return createProcTimeCommitPredicate(conf);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported partition commit predicate: " + trigger);
        }
    }
}

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

import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY;

/**
 * Partition commit trigger by creation time and processing time service, if 'current processing
 * time' > 'partition creation time' + 'delay', the partition is committable.
 */
public class ProcTimeCommitPredicate implements PartitionCommitPredicate {
    private final long commitDelay;

    public ProcTimeCommitPredicate(Configuration conf) {
        this.commitDelay = conf.get(SINK_PARTITION_COMMIT_DELAY).toMillis();
    }

    @Override
    public boolean isPartitionCommittable(PredicateContext predicateContext) {
        long currentProcTime = predicateContext.currentProcTime();
        return commitDelay == 0
                || currentProcTime > predicateContext.createProcTime() + commitDelay;
    }
}

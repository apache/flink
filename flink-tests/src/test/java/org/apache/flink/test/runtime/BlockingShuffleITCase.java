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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.junit.jupiter.api.Test;

/** Tests for blocking shuffle. */
class BlockingShuffleITCase extends BatchShuffleITCaseBase {

    @Test
    public void testBoundedBlockingShuffle() throws Exception {
        final int numRecordsToSend = 1000000;
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM,
                Integer.MAX_VALUE);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    public void testBoundedBlockingShuffleWithoutData() throws Exception {
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM,
                Integer.MAX_VALUE);
        JobGraph jobGraph = createJobGraph(0, false, false, configuration);
        executeJob(jobGraph, configuration, 0);
    }

    @Test
    public void testSortMergeBlockingShuffle() throws Exception {
        final int numRecordsToSend = 1000000;
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS, 64);

        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    public void testSortMergeBlockingShuffleWithoutData() throws Exception {
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS, 64);

        JobGraph jobGraph = createJobGraph(0, false, false, configuration);
        executeJob(jobGraph, configuration, 0);
    }

    @Test
    public void testDeletePartitionFileOfBoundedBlockingShuffle() throws Exception {
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM,
                Integer.MAX_VALUE);

        JobGraph jobGraph = createJobGraph(0, false, true, configuration);
        executeJob(jobGraph, configuration, 0);
    }

    @Test
    public void testDeletePartitionFileOfSortMergeBlockingShuffle() throws Exception {
        Configuration configuration = getConfiguration();
        JobGraph jobGraph = createJobGraph(0, false, true, configuration);
        executeJob(jobGraph, configuration, 0);
    }

    @Override
    protected Configuration getConfiguration() {
        Configuration configuration = super.getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
        return configuration;
    }
}

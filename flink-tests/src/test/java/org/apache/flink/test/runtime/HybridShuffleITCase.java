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

/** Tests for hybrid shuffle mode. */
class HybridShuffleITCase extends BatchShuffleITCaseBase {

    @Test
    void testHybridFullExchanges() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE, false);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testHybridSelectiveExchanges() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE, false);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testHybridFullExchangesRestart() throws Exception {
        final int numRecordsToSend = 10;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE, false);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @Test
    void testHybridSelectiveExchangesRestart() throws Exception {
        final int numRecordsToSend = 10;
        Configuration configuration = getConfiguration();
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE, false);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }
}

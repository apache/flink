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
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

/** Tests for hybrid shuffle mode. */
@ExtendWith(ParameterizedTestExtension.class)
class HybridShuffleITCase extends BatchShuffleITCaseBase {

    @Parameter public boolean enableNewHybridMode;

    @Parameters(name = "enableNewHybridMode={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @TestTemplate
    void testHybridFullExchanges() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = configureHybridOptions(getConfiguration(), false);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @TestTemplate
    void testHybridSelectiveExchanges() throws Exception {
        final int numRecordsToSend = 10000;
        Configuration configuration = configureHybridOptions(getConfiguration(), true);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, false, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @TestTemplate
    void testHybridFullExchangesRestart() throws Exception {
        final int numRecordsToSend = 10;
        Configuration configuration = configureHybridOptions(getConfiguration(), false);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    @TestTemplate
    void testHybridSelectiveExchangesRestart() throws Exception {
        final int numRecordsToSend = 10;
        Configuration configuration = configureHybridOptions(getConfiguration(), true);
        JobGraph jobGraph = createJobGraph(numRecordsToSend, true, configuration);
        executeJob(jobGraph, configuration, numRecordsToSend);
    }

    private Configuration configureHybridOptions(Configuration configuration, boolean isSelective) {
        BatchShuffleMode shuffleMode =
                isSelective
                        ? BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE
                        : BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL;
        configuration.set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE,
                enableNewHybridMode);

        if (enableNewHybridMode && isSelective) {
            // Note that the memory tier of the new mode need more buffers for the selective mode
            configuration.setString(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "128m");
        }
        return configuration;
    }
}

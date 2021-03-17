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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

/** Utils to run {@link JobGraph} on {@link MiniCluster}. */
public class JobGraphRunningUtil {

    public static void execute(
            JobGraph jobGraph,
            Configuration configuration,
            int numTaskManagers,
            int numSlotsPerTaskManager)
            throws Exception {
        configuration.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("1g"));
        configuration.setString(RestOptions.BIND_PORT, "0");

        final MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumTaskManagers(numTaskManagers)
                        .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
            miniCluster.start();

            MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);
            // wait for the submission to succeed
            JobID jobID = miniClusterClient.submitJob(jobGraph).get();

            JobResult jobResult = miniClusterClient.requestJobResult(jobID).get();
            if (jobResult.getSerializedThrowable().isPresent()) {
                throw new AssertionError(jobResult.getSerializedThrowable().get());
            }
        }
    }
}

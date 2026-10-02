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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MdcOptions;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.apache.flink.test.misc.JobIDLoggingITCase.getConfiguration;
import static org.apache.flink.test.misc.JobIDLoggingITCase.runJob;
import static org.apache.flink.util.JobIDLoggingUtil.assertKeyPresent;
import static org.slf4j.event.Level.DEBUG;

class EnrichedMdcLoggingITCase {
    @RegisterExtension
    public final LoggerAuditingExtension jobMasterLogging =
            new LoggerAuditingExtension(JobMaster.class, DEBUG);

    @RegisterExtension
    public final LoggerAuditingExtension taskExecutorLogging =
            new LoggerAuditingExtension(TaskExecutor.class, DEBUG);

    @RegisterExtension
    public static MiniClusterExtension miniClusterResource =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Test
    void testEnrichedMdcLogging(@InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        final Configuration enrichmentConfig = new Configuration();
        enrichmentConfig.set(
                MdcOptions.JOB_CONFIGURATION_TO_MDC_KEYS, Map.of("job.key-1", "mdc-key-1"));
        enrichmentConfig.setString("job.key-1", "val-1");

        final JobID jobID = runJob(clusterClient, enrichmentConfig);
        clusterClient.cancel(jobID).get();

        assertKeyPresent(
                "mdc-key-1",
                "val-1",
                jobMasterLogging,
                asList("Initializing job .*", "Starting execution of job .*"),
                "Registration at ResourceManager.*",
                "Registration with ResourceManager.*",
                "Resolved ResourceManager address.*");

        assertKeyPresent(
                "mdc-key-1",
                "val-1",
                taskExecutorLogging,
                asList("Received task .*"),
                "TaskManager received a checkpoint confirmation for unknown task.*",
                "TaskManager received an aborted checkpoint for unknown task.*",
                "Un-registering task.*",
                "Successful registration.*",
                "Establish JobManager connection.*",
                "Offer reserved slots.*",
                ".*ResourceManager.*",
                "Operator event.*",
                "Recovered slot allocation snapshots.*",
                ".*heartbeat.*",
                ".*leadership.*",
                "Freeing inactive slots.*");
    }
}

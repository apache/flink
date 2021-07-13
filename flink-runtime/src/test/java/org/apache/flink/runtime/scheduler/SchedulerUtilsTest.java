/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link SchedulerUtils} utilities. */
public class SchedulerUtilsTest extends TestLogger {

    private ClassLoader classLoader = getClass().getClassLoader();
    private JobID jobID = new JobID();
    private JobGraph jobGraph = new JobGraph(jobID, "jobName");
    private RecoveryRecordingCompletedCheckpointStore expectedCompletedCheckpointStore =
            new RecoveryRecordingCompletedCheckpointStore();
    private CheckpointRecoveryFactory checkpointRecoveryFactory =
            new TestingCheckpointRecoveryFactory(expectedCompletedCheckpointStore, null);

    @Before
    public void setUp() throws Exception {
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        new CheckpointCoordinatorConfiguration
                                        .CheckpointCoordinatorConfigurationBuilder()
                                .build(),
                        null));
    }

    @Test
    public void testSettingMaxNumberOfCheckpointsToRetain() throws Exception {

        final int maxNumberOfCheckpointsToRetain = 10;
        final Configuration jobManagerConfig = new Configuration();
        jobManagerConfig.setInteger(
                CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, maxNumberOfCheckpointsToRetain);

        final CompletedCheckpointStore actualCompletedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                        jobGraph, jobManagerConfig, classLoader, checkpointRecoveryFactory, log);

        assertTrue(expectedCompletedCheckpointStore.recovered);
        assertEquals(expectedCompletedCheckpointStore, actualCompletedCheckpointStore);
    }

    private static class RecoveryRecordingCompletedCheckpointStore
            extends StandaloneCompletedCheckpointStore {
        private volatile boolean recovered = false;

        public RecoveryRecordingCompletedCheckpointStore() {
            super(1);
        }

        @Override
        public void recover() throws Exception {
            recovered = true;
        }
    }
}

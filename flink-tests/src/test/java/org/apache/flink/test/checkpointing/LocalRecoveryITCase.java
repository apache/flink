/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.test.checkpointing.EventTimeWindowCheckpointingITCase.StateBackendEnum;
import static org.apache.flink.test.checkpointing.EventTimeWindowCheckpointingITCase.StateBackendEnum.FILE;
import static org.apache.flink.test.checkpointing.EventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_FULL;
import static org.apache.flink.test.checkpointing.EventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_INCREMENTAL_ZK;

/**
 * This test delegates to instances of {@link EventTimeWindowCheckpointingITCase} that have been
 * reconfigured to use local recovery.
 *
 * <p>TODO: This class must be refactored to properly extend {@link
 * EventTimeWindowCheckpointingITCase}.
 */
@RunWith(Parameterized.class)
public class LocalRecoveryITCase extends TestLogger {

    @Rule public TestName testName = new TestName();

    @Parameterized.Parameter public StateBackendEnum backendEnum;

    @Parameterized.Parameters(name = "statebackend type ={0}")
    public static Collection<StateBackendEnum> parameter() {
        return Arrays.asList(ROCKSDB_FULL, ROCKSDB_INCREMENTAL_ZK, FILE);
    }

    @Test
    public final void executeTest() throws Exception {
        EventTimeWindowCheckpointingITCase.tempFolder.create();
        EventTimeWindowCheckpointingITCase windowChkITCase =
                new EventTimeWindowCheckpointingITCaseInstance(backendEnum, true);

        executeTest(windowChkITCase);
    }

    private void executeTest(EventTimeWindowCheckpointingITCase delegate) throws Exception {
        delegate.name = testName;
        delegate.stateBackendEnum = backendEnum;
        try {
            delegate.setupTestCluster();
            try {
                delegate.testTumblingTimeWindow();
                delegate.stopTestCluster();
            } catch (Exception e) {
                delegate.stopTestCluster();
                throw new RuntimeException(e);
            }

            delegate.setupTestCluster();
            try {
                delegate.testSlidingTimeWindow();
                delegate.stopTestCluster();
            } catch (Exception e) {
                delegate.stopTestCluster();
                throw new RuntimeException(e);
            }
        } finally {
            EventTimeWindowCheckpointingITCase.tempFolder.delete();
        }
    }

    @Ignore("Prevents this class from being considered a test class by JUnit.")
    private static class EventTimeWindowCheckpointingITCaseInstance
            extends EventTimeWindowCheckpointingITCase {

        private final StateBackendEnum backendEnum;
        private final boolean localRecoveryEnabled;

        public EventTimeWindowCheckpointingITCaseInstance(
                StateBackendEnum backendEnum, boolean localRecoveryEnabled) {
            super(backendEnum, 2);

            this.backendEnum = backendEnum;
            this.localRecoveryEnabled = localRecoveryEnabled;
        }

        @Override
        protected StateBackendEnum getStateBackend() {
            return backendEnum;
        }

        @Override
        protected Configuration createClusterConfig() throws IOException {
            Configuration config = super.createClusterConfig();

            config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, localRecoveryEnabled);

            return config;
        }
    }
}

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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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

    @Parameterized.Parameter(1)
    public boolean localRecoveryEnabled;

    @Parameterized.Parameter(2)
    public boolean localBackupEnabled;

    private static final List<StateBackendEnum> STATE_BACKEND_ENUMS =
            Arrays.asList(ROCKSDB_FULL, ROCKSDB_INCREMENTAL_ZK, FILE);

    private static final List<Tuple2<Boolean, Boolean>> LOCAL_BACKUP_AND_RECOVERY_CONFIGS =
            Arrays.asList(Tuple2.of(true, true), Tuple2.of(true, false), Tuple2.of(false, true));

    @Parameterized.Parameters(
            name = "stateBackendType = {0}, localBackupEnabled = {1}, localRecoveryEnabled = {2}")
    public static Collection<Object[]> parameter() {
        List<Object[]> parameterList = new ArrayList<>();
        for (StateBackendEnum stateBackend : STATE_BACKEND_ENUMS) {
            for (Tuple2<Boolean, Boolean> backupAndRecoveryConfig :
                    LOCAL_BACKUP_AND_RECOVERY_CONFIGS) {
                parameterList.add(
                        new Object[] {
                            stateBackend, backupAndRecoveryConfig.f0, backupAndRecoveryConfig.f1
                        });
            }
        }
        return parameterList;
    }

    @Test
    public final void executeTest() throws Exception {
        EventTimeWindowCheckpointingITCase.tempFolder.create();
        EventTimeWindowCheckpointingITCase windowChkITCase =
                new EventTimeWindowCheckpointingITCaseInstance(
                        backendEnum, localBackupEnabled, localRecoveryEnabled);

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
        private final boolean localBackupEnable;
        private final boolean localRecoveryEnabled;

        public EventTimeWindowCheckpointingITCaseInstance(
                StateBackendEnum backendEnum,
                boolean localBackupEnable,
                boolean localRecoveryEnabled) {
            super(backendEnum, 2);
            this.backendEnum = backendEnum;
            this.localBackupEnable = localBackupEnable;
            this.localRecoveryEnabled = localRecoveryEnabled;
        }

        @Override
        protected StateBackendEnum getStateBackend() {
            return backendEnum;
        }

        @Override
        protected Configuration createClusterConfig() throws IOException {
            Configuration config = super.createClusterConfig();
            config.set(StateRecoveryOptions.LOCAL_RECOVERY, localRecoveryEnabled);
            config.set(CheckpointingOptions.LOCAL_BACKUP_ENABLED, localBackupEnable);
            return config;
        }
    }
}

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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;

import static org.apache.flink.runtime.state.LocalRecoveryConfig.LocalRecoveryMode;
import static org.apache.flink.runtime.state.LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED;
import static org.apache.flink.test.checkpointing.AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum;
import static org.apache.flink.test.checkpointing.AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum.FILE_ASYNC;
import static org.apache.flink.test.checkpointing.AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_FULLY_ASYNC;
import static org.apache.flink.test.checkpointing.AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_INCREMENTAL_ZK;

/**
 * This test delegates to instances of {@link AbstractEventTimeWindowCheckpointingITCase} that have been reconfigured
 * to use local recovery.
 */
public class LocalRecoveryITCase extends TestLogger {

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testLocalRecoveryHeapBackendFileBased() throws Exception {
		executeTest(
			FILE_ASYNC,
			ENABLE_FILE_BASED);
	}

	@Test
	public void testLocalRecoveryRocksIncrementalFileBased() throws Exception {
		executeTest(
			ROCKSDB_INCREMENTAL_ZK,
			ENABLE_FILE_BASED);
	}

	@Test
	public void testLocalRecoveryRocksFullFileBased() throws Exception {
		executeTest(
			ROCKSDB_FULLY_ASYNC,
			ENABLE_FILE_BASED);
	}

	private void executeTest(
		StateBackendEnum backendEnum,
		LocalRecoveryMode recoveryMode) throws Exception {

		AbstractEventTimeWindowCheckpointingITCase windowChkITCase =
			new AbstractEventTimeWindowCheckpointingITCaseWithLocalRecovery(
				backendEnum,
				recoveryMode);

		executeTest(windowChkITCase);
	}

	private void executeTest(AbstractEventTimeWindowCheckpointingITCase delegate) throws Exception {
		delegate.name = testName;
		delegate.tempFolder.create();
		try {
			delegate.startTestCluster();
			delegate.testTumblingTimeWindow();
			delegate.stopTestCluster();

			delegate.startTestCluster();
			delegate.testSlidingTimeWindow();
			delegate.stopTestCluster();
		} finally {
			delegate.tempFolder.delete();
		}
	}

	private static class AbstractEventTimeWindowCheckpointingITCaseWithLocalRecovery
		extends AbstractEventTimeWindowCheckpointingITCase {

		private final LocalRecoveryMode recoveryMode;

		AbstractEventTimeWindowCheckpointingITCaseWithLocalRecovery(
			StateBackendEnum stateBackendEnum,
			LocalRecoveryMode recoveryMode) {

			super(stateBackendEnum);
			this.recoveryMode = recoveryMode;
		}

		@Override
		protected Configuration createClusterConfig() throws IOException {
			Configuration config = super.createClusterConfig();

			config.setString(
				CheckpointingOptions.LOCAL_RECOVERY,
				recoveryMode.toString());

			return config;
		}
	}
}

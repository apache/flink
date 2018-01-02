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

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * This test delegates to instances of {@link AbstractEventTimeWindowCheckpointingITCase} that have been reconfigured
 * to use local recovery.
 */
public class LocalRecoveryITCase extends TestLogger {

	@Rule
	public TestName testName = new TestName();

	@Test
	public void testLocalRecoveryHeapBackendFileBased() throws Exception {
		AbstractEventTimeWindowCheckpointingITCase windowChkITCase =
			new AbstractEventTimeWindowCheckpointingITCase(
				AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum.FILE_ASYNC) {

				@Override
				public void startTestCluster() throws Exception {
					super.startTestCluster();
					FsStateBackend backend = (FsStateBackend) this.stateBackend;
					backend.setLocalRecoveryMode(FsStateBackend.LocalRecoveryMode.ENABLE_FILE_BASED);
				}
			};

		executeTest(windowChkITCase);
	}

	@Test
	public void testLocalRecoveryRocksIncrementalFileBased() throws Exception {
		AbstractEventTimeWindowCheckpointingITCase windowChkITCase =
			new AbstractEventTimeWindowCheckpointingITCase(
				AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_INCREMENTAL_ZK) {

				@Override
				public void startTestCluster() throws Exception {
					super.startTestCluster();
					RocksDBStateBackend backend = (RocksDBStateBackend) this.stateBackend;
					backend.setLocalRecoveryMode(RocksDBStateBackend.LocalRecoveryMode.ENABLE_FILE_BASED);
				}
			};

		executeTest(windowChkITCase);
	}

	@Test
	public void testLocalRecoveryRocksFullFileBased() throws Exception {
		AbstractEventTimeWindowCheckpointingITCase windowChkITCase =
			new AbstractEventTimeWindowCheckpointingITCase(
				AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_FULLY_ASYNC) {

				@Override
				public void startTestCluster() throws Exception {
					super.startTestCluster();
					RocksDBStateBackend backend = (RocksDBStateBackend) this.stateBackend;
					backend.setLocalRecoveryMode(RocksDBStateBackend.LocalRecoveryMode.ENABLE_FILE_BASED);
				}
			};

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
}

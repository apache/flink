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
import static org.apache.flink.test.checkpointing.AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum;

/**
 * This test delegates to instances of {@link AbstractEventTimeWindowCheckpointingITCase} that have been reconfigured
 * to use local recovery.
 *
 * <p>TODO: This class must be refactored to properly extend {@link AbstractEventTimeWindowCheckpointingITCase}.
 */
public abstract class AbstractLocalRecoveryITCase extends TestLogger {

	private final StateBackendEnum backendEnum;
	private final LocalRecoveryMode recoveryMode;

	@Rule
	public TestName testName = new TestName();

	AbstractLocalRecoveryITCase(StateBackendEnum backendEnum, LocalRecoveryMode recoveryMode) {
		this.backendEnum = backendEnum;
		this.recoveryMode = recoveryMode;
	}

	@Test
	public final void executeTest() throws Exception {
		AbstractEventTimeWindowCheckpointingITCase.tempFolder.create();
		AbstractEventTimeWindowCheckpointingITCase windowChkITCase =
			new AbstractEventTimeWindowCheckpointingITCase() {
				@Override
				protected StateBackendEnum getStateBackend() {
					return backendEnum;
				}

				@Override
				protected Configuration createClusterConfig() throws IOException {
					Configuration config = super.createClusterConfig();

					config.setString(
						CheckpointingOptions.LOCAL_RECOVERY,
						recoveryMode.toString());

					return config;
				}
			};

		executeTest(windowChkITCase);
	}

	private void executeTest(AbstractEventTimeWindowCheckpointingITCase delegate) throws Exception {
		delegate.name = testName;
		try {
			delegate.setupTestCluster();
			try {
				delegate.testTumblingTimeWindow();
				delegate.stopTestCluster();
			} catch (Exception e) {
				delegate.stopTestCluster();
			}

			delegate.setupTestCluster();
			try {
				delegate.testSlidingTimeWindow();
				delegate.stopTestCluster();
			} catch (Exception e) {
				delegate.stopTestCluster();
			}
		} finally {
			delegate.tempFolder.delete();
		}
	}
}

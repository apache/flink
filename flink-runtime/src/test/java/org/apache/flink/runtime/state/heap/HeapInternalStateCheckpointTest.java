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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.InternalStateCheckpointTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Unit tests to validates that states can be correctly saved and
 * restored in {@link HeapInternalStateBackend}.
 */
@RunWith(Parameterized.class)
public class HeapInternalStateCheckpointTest extends InternalStateCheckpointTestBase {

	@Parameterized.Parameters(name = "checkpointType: {0}, asyncSnapshot: {1}")
	public static Collection<Object[]> checkpointTypes() {
		Collection<Object[]> parameter = new ArrayList<>();

		for (CheckpointType type : CheckpointType.values()) {
			parameter.add(new Object[] {type, false});
			parameter.add(new Object[] {type, true});
		}
		return parameter;
	}

	@Parameterized.Parameter
	public CheckpointType checkpointType;

	@Parameterized.Parameter(1)
	public boolean asyncSnapshot;

	@Override
	protected AbstractInternalStateBackend createStateBackend(
		int numberOfGroups,
		KeyGroupRange keyGroupRange,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		ExecutionConfig executionConfig) {
		return new HeapInternalStateBackend(
			numberOfGroups,
			keyGroupRange,
			userClassLoader,
			localRecoveryConfig,
			null,
			asyncSnapshot,
			executionConfig);
	}

	@Override
	protected CheckpointType getCheckpointType() {
		return checkpointType;
	}
}

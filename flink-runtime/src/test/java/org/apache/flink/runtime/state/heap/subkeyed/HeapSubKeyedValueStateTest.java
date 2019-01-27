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

package org.apache.flink.runtime.state.heap.subkeyed;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateTestBase;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Unit tests for {@link SubKeyedValueState} backed by {@link HeapInternalStateBackend}.
 */
@RunWith(Parameterized.class)
public class HeapSubKeyedValueStateTest extends SubKeyedValueStateTestBase {

	@Parameterized.Parameter
	public boolean asyncSnapshot;

	@Parameterized.Parameters(name = "asyncSnapshot={0}")
	public static Collection<Object[]> parameters() {
		return Arrays.asList(new Object[][]{{true}, {false}});
	}

	@Override
	protected AbstractInternalStateBackend createStateBackend(
		int numberOfGroups,
		KeyGroupRange keyGroupRange,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig
	) throws Exception {
		return new HeapInternalStateBackend(
			numberOfGroups,
			keyGroupRange,
			userClassLoader,
			localRecoveryConfig,
			null,
			asyncSnapshot,
			new ExecutionConfig());
	}
}

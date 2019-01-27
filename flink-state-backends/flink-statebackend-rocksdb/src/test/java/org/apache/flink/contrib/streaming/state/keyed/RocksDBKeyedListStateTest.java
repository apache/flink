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

package org.apache.flink.contrib.streaming.state.keyed;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.contrib.streaming.state.RocksDBInternalStateBackend;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.runtime.state.keyed.KeyedListStateTestBase;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

/**
 * Unit tests for {@link KeyedListState} backed by {@link RocksDBInternalStateBackend}.
 */
public class RocksDBKeyedListStateTest extends KeyedListStateTestBase {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Override
	protected AbstractInternalStateBackend createStateBackend(
		int numberOfGroups,
		KeyGroupRange keyGroupRange,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig
	) throws Exception {
		return new RocksDBInternalStateBackend(
			userClassLoader,
			temporaryFolder.newFolder().getAbsoluteFile(),
			new DBOptions().setCreateIfMissing(true),
			new ColumnFamilyOptions(),
			numberOfGroups,
			keyGroupRange,
			true,
			localRecoveryConfig,
			null,
			new ExecutionConfig()
			);
	}

}


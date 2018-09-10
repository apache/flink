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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.StateBackendTestContext;
import org.apache.flink.runtime.state.ttl.TtlStateTestBase;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/** Test suite for rocksdb state TTL. */
public class RocksDBTtlStateTest extends TtlStateTestBase {
	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected StateBackendTestContext createStateBackendTestContext(TtlTimeProvider timeProvider) {
		return new StateBackendTestContext(timeProvider) {
			@Override
			protected StateBackend createStateBackend() {
				return RocksDBTtlStateTest.this.createStateBackend();
			}
		};
	}

	private StateBackend createStateBackend() {
		String dbPath;
		String checkpointPath;
		try {
			dbPath = tempFolder.newFolder().getAbsolutePath();
			checkpointPath = tempFolder.newFolder().toURI().toString();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to init rocksdb test state backend");
		}
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), TernaryBoolean.FALSE);
		backend.setDbStoragePath(dbPath);
		return backend;
	}
}

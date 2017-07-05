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

package org.apache.flink.runtime.zookeeper.filesystem;

import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileNotFoundException;

/**
 * Tests for basic {@link FileSystemStateStorageHelper#closeAndCleanupAllData} behaviour.
 */
public class FileSystemStateStorageHelperTest extends TestLogger {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testCleanUpAllData() throws Exception {
		FileSystemStateStorageHelper<String> stateStorage1 = new FileSystemStateStorageHelper<>("/tmp", "storage1");
		FileSystemStateStorageHelper<String> stateStorage2 = new FileSystemStateStorageHelper<>("/tmp", "storage2");
		String storage1Data = "data of storage1", storage2Data = "data of storage2";

		RetrievableStateHandle<String> stateHandle1 = stateStorage1.store(storage1Data);
		RetrievableStateHandle<String> stateHandle2 = stateStorage2.store(storage2Data);

		Assert.assertEquals(stateHandle1.retrieveState(), storage1Data);
		Assert.assertEquals(stateHandle2.retrieveState(), storage2Data);

		stateStorage1.closeAndCleanupAllData();
		Assert.assertEquals(stateHandle2.retrieveState(), storage2Data);
		stateStorage2.closeAndCleanupAllData();

		expectedException.expect(FileNotFoundException.class);
		expectedException.expectMessage("storage1");
		stateHandle1.retrieveState();
	}
}

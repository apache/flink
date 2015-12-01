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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;

/**
 * Basic {@link FileSystemStateStore} behaviour test.
 */
public class FileSystemStateStoreTest extends AbstractStateStoreTest {

	private static File rootPath;

	@BeforeClass
	public static void setUp() throws Exception {
		rootPath = CommonTestUtils.createTempDirectory();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (rootPath != null) {
			try {
				String errMsg = "Root path " + rootPath.getPath() + " not cleaned up. "
						+ Arrays.toString(rootPath.listFiles());
				assertFalse(errMsg, rootPath.exists());
			}
			finally {
				org.apache.commons.io.FileUtils.deleteDirectory(rootPath);
			}
		}
	}

	@Override
	StateStore<Integer> createStateStore() throws Exception {
		return new FileSystemStateStore<>(new Path(rootPath.toURI()), "test_savepoint-");
	}

	@Override
	@SuppressWarnings("unchecked")
	boolean verifyDiscarded(StateStore<Integer> stateStore, String path) {
		return true;
	}

}

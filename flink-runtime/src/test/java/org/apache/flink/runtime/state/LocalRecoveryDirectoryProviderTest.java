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

package org.apache.flink.runtime.state;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class LocalRecoveryDirectoryProviderTest {

	private static final String SPECIFIC_PATH = "specificPath";

	private TemporaryFolder tmpFolder;
	private LocalRecoveryDirectoryProvider directoryProvider;
	private File[] rootFolders;

	@Before
	public void setup() throws IOException {
		this.tmpFolder = new TemporaryFolder();
		this.tmpFolder.create();
		this.rootFolders = new File[]{tmpFolder.newFolder(), tmpFolder.newFolder(), tmpFolder.newFolder()};
		this.directoryProvider = new LocalRecoveryDirectoryProvider(
			rootFolders,
			SPECIFIC_PATH);
	}

	@After
	public void tearDown() {
		this.tmpFolder.delete();
	}

	@Test
	public void nextRootDirectory() throws Exception {
		for (int i = 0; i < 10; ++i) {
			Assert.assertEquals(rootFolders[i % rootFolders.length], directoryProvider.nextRootDirectory());
		}
	}

	@Test
	public void selectRootDirectory() throws Exception {
		for (int i = 0; i < rootFolders.length; ++i) {
			Assert.assertEquals(rootFolders[i], directoryProvider.selectRootDirectory(i));
		}
	}

	@Test
	public void rootDirectoryCount() throws Exception {
		Assert.assertEquals(rootFolders.length, directoryProvider.rootDirectoryCount());
	}

	@Test
	public void getSubtaskSpecificPath() throws Exception {
		Assert.assertEquals(SPECIFIC_PATH, directoryProvider.getSubtaskSpecificPath());
	}

	@Test
	public void testPreconditionsNotNullFiles() {
		try {
			new LocalRecoveryDirectoryProvider(new File[]{null}, SPECIFIC_PATH);
			Assert.fail();
		} catch (NullPointerException ignore) {
		}
	}

	@Test
	public void testPreconditionsNonExistingFolder() {
		try {
			new LocalRecoveryDirectoryProvider(new File[]{new File("123")}, SPECIFIC_PATH);
			Assert.fail();
		} catch (IllegalStateException ignore) {
		}
	}

}

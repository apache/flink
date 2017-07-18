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

package org.apache.flink.runtime.blob;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

import org.apache.flink.util.OperatingSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;

import java.io.File;
import java.io.IOException;
import org.junit.rules.TemporaryFolder;

public class BlobUtilsTest {

	private final static String CANNOT_CREATE_THIS = "cannot-create-this";

	private File blobUtilsTestDirectory;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void before() throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		// Prepare test directory
		blobUtilsTestDirectory = temporaryFolder.newFolder();
		assertTrue(blobUtilsTestDirectory.setExecutable(true, false));
		assertTrue(blobUtilsTestDirectory.setReadable(true, false));
		assertTrue(blobUtilsTestDirectory.setWritable(false, false));
	}

	@After
	public void after() {
		// Cleanup test directory
		assertTrue(blobUtilsTestDirectory.delete());
	}

	@Test(expected = IOException.class)
	public void testExceptionOnCreateStorageDirectoryFailure() throws
		IOException {
		// Should throw an Exception
		BlobUtils.initStorageDirectory(new File(blobUtilsTestDirectory, CANNOT_CREATE_THIS).getAbsolutePath());
	}

	@Test(expected = Exception.class)
	public void testExceptionOnCreateCacheDirectoryFailure() {
		// Should throw an Exception
		BlobUtils.getStorageLocation(new File(blobUtilsTestDirectory, CANNOT_CREATE_THIS), mock(BlobKey.class));
	}
}

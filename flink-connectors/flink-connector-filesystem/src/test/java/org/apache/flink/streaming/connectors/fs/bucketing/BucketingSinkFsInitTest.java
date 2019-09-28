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

package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.UnsupportedFileSystemSchemeException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the file system initialization of the Bucketing sink.
 *
 * <p>NOTE: These tests can probably go away once the bucketing sink has been
 * migrated to properly use Flink's file system abstraction.
 */
public class BucketingSinkFsInitTest {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	// ------------------------------------------------------------------------

	// to properly mimic what happens in the runtime task, we need to make sure that
	// the file system safety net is in place

	@Before
	public void activateSafetyNet() {
		FileSystemSafetyNet.initializeSafetyNetForThread();
	}

	@After
	public void deactivateSafetyNet() {
		FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
	}

	// ------------------------------------------------------------------------

	@Test
	public void testInitForLocalFileSystem() throws Exception {
		final Path path = new Path(tempFolder.newFolder().toURI());
		FileSystem fs = BucketingSink.createHadoopFileSystem(path, null);

		assertEquals("file", fs.getUri().getScheme());
	}

	@Test
	public void testInitForHadoopFileSystem() throws Exception {
		final Path path = new Path("hdfs://localhost:51234/some/path/");
		FileSystem fs = BucketingSink.createHadoopFileSystem(path, null);

		assertEquals("hdfs", fs.getUri().getScheme());
	}

	@Test(expected = UnsupportedFileSystemSchemeException.class)
	public void testInitForUnsupportedFileSystem() throws Exception {
		final Path path = new Path("nofs://localhost:51234/some/path/");
		BucketingSink.createHadoopFileSystem(path, null);
	}
}

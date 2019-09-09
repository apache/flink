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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link AbstractUserClassPathJobGraphRetriever}.
 */
public class AbstractUserClassPathJobGraphRetrieverTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Test class.
	 */
	public static class TestJobGraphRetrieverTest extends AbstractUserClassPathJobGraphRetriever {

		public TestJobGraphRetrieverTest(String jobDir) {
			super(jobDir);
		}

		@Override
		public JobGraph retrieveJobGraph(Configuration configuration) {
			return null;
		}
	}

	@Test
	public void testGetUserClassPathFromAFile() throws IOException {
		final String fileName = "a.jar";
		File file = temporaryFolder.newFile(fileName);

		TestJobGraphRetrieverTest testJobGraphRetrieverTest = new TestJobGraphRetrieverTest(file.getAbsolutePath());

		assertEquals(Collections.emptyList(), testJobGraphRetrieverTest.getUserClassPaths());
	}

	@Test
	public void testGetUserClassPathFormADoesNotExistsFile() throws IOException {
		final String fileName = "_does_not_exists_file";
		final String doesNotExistsFilePath = temporaryFolder.getRoot() + "/" + fileName;

		TestJobGraphRetrieverTest testJobGraphRetrieverTest = new TestJobGraphRetrieverTest(doesNotExistsFilePath);

		assertEquals(Collections.emptyList(), testJobGraphRetrieverTest.getUserClassPaths());

	}

	@Test
	public void testGetUserClassPath() throws IOException {

		final Path jobDir = temporaryFolder.newFolder("_job_dir").toPath();
		final Path jobSubDir1 = Files.createDirectory(jobDir.resolve("_sub_dir1"));
		final Path jobSubDir2 = Files.createDirectory(jobDir.resolve("_sub_dir2"));
		final Path jarFile1 = Files.createFile(jobDir.resolve("file1.jar"));
		final Path jarFile2 = Files.createFile(jobDir.resolve("file2.jar"));
		final Path jarFile3 = Files.createFile(jobSubDir1.resolve("file3.jar"));
		final Path jarFile4 = Files.createFile(jobSubDir2.resolve("file4.jar"));

		Files.createFile(jobDir.resolve("file1.txt"));
		Files.createFile(jobSubDir2.resolve("file2.txt"));

		List<URL> expectedUrls = Arrays.asList(jarFile1.toUri().toURL(),
			jarFile2.toUri().toURL(),
			jarFile3.toUri().toURL(),
			jarFile4.toUri().toURL());

		TestJobGraphRetrieverTest testJobGraphRetrieverTest =
			new TestJobGraphRetrieverTest(jobDir.toString());

		assertTrue(CollectionUtils.isEqualCollection(expectedUrls, testJobGraphRetrieverTest.getUserClassPaths()));
	}

}

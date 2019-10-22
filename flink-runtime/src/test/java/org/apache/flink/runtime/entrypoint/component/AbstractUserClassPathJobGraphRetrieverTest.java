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
import org.apache.flink.util.TestLogger;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link AbstractUserClassPathJobGraphRetriever}.
 */
public class AbstractUserClassPathJobGraphRetrieverTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Test class.
	 */
	private static class TestJobGraphRetriever extends AbstractUserClassPathJobGraphRetriever {
		public TestJobGraphRetriever(String jobDir) {
			super(jobDir);
		}

		@Override
		public JobGraph retrieveJobGraph(Configuration configuration) {
			return null;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetUserClassPathFromAFile() throws IOException {
		final String fileName = "a.jar";
		File file = temporaryFolder.newFile(fileName);

		TestJobGraphRetriever testJobGraphRetriever = new TestJobGraphRetriever(file.getAbsolutePath());

		testJobGraphRetriever.getUserClassPaths();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetUserClassPathFormADoesNotExistsFile() throws IOException {
		final String fileName = "_does_not_exists_file";
		final String doesNotExistsFilePath = temporaryFolder.getRoot() + "/" + fileName;

		TestJobGraphRetriever testJobGraphRetriever = new TestJobGraphRetriever(doesNotExistsFilePath);

		assertEquals(Collections.emptyList(), testJobGraphRetriever.getUserClassPaths());
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

		Path userDir = Paths.get(System.getProperty("user.dir"));
		URL spec = new URL(jobDir.toUri().getScheme() + ":");
		List<URL> expectedUrls = Arrays.asList(
			new URL(spec, userDir.relativize(jarFile1).toString()),
			new URL(spec, userDir.relativize(jarFile2).toString()),
			new URL(spec, userDir.relativize(jarFile3).toString()),
			new URL(spec, userDir.relativize(jarFile4).toString()));

		TestJobGraphRetriever testJobGraphRetriever =
			new TestJobGraphRetriever(jobDir.toString());

		assertTrue(CollectionUtils.isEqualCollection(expectedUrls, testJobGraphRetriever.getUserClassPaths()));
	}

	@Test
	public void testGetUserClassPathWithRelativeJobDir() throws IOException {
		Path jobRelativeDir = Paths.get("_job_dir");
		Path jobRelativeSubDir = Paths.get("_job_dir", "_sub_dir");
		Path jarFile1 = Paths.get(jobRelativeDir.toString(), "file1.jar");
		Path jarFile2 = Paths.get(jobRelativeSubDir.toString(), "file2.jar");

		URL context = new URL(jobRelativeDir.toUri().getScheme() + ":");
		List<URL> expectedURLs = Arrays.asList(
			new URL(context, jarFile1.toString()), new URL(context, jarFile2.toString()));

		try {
			if (!Files.exists(jobRelativeDir)) {
				Files.createDirectory(jobRelativeDir);
			}
			if (!Files.exists(jobRelativeSubDir)) {
				Files.createDirectory(jobRelativeSubDir);
			}
			if (!Files.exists(jarFile1)) {
				Files.createFile(jarFile1);
			}
			if (!Files.exists(jarFile2)) {
				Files.createFile(jarFile2);
			}

			TestJobGraphRetriever testJobGraphRetriever =
				new TestJobGraphRetriever(jobRelativeDir.toString());
			assertTrue(CollectionUtils.isEqualCollection(expectedURLs, testJobGraphRetriever.getUserClassPaths()));
		} finally {
			Files.deleteIfExists(jarFile1);
			Files.deleteIfExists(jarFile2);
			Files.deleteIfExists(jobRelativeSubDir);
			Files.deleteIfExists(jobRelativeDir);
		}
	}

}

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

package org.apache.flink.yarn;

import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.yarn.YarnTestUtils.generateFilesInDirectory;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link YarnApplicationFileUploader}.
 */
public class YarnApplicationFileUploaderTest extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testRegisterProvidedLocalResources() throws IOException {
		final File flinkLibDir = temporaryFolder.newFolder();
		final Map<String, String> libJars = getLibJars();

		generateFilesInDirectory(flinkLibDir, libJars);

		try (final YarnApplicationFileUploader yarnApplicationFileUploader = YarnApplicationFileUploader.from(
				FileSystem.get(new YarnConfiguration()),
				new Path(temporaryFolder.getRoot().toURI()),
				Collections.singletonList(new Path(flinkLibDir.toURI())),
				ApplicationId.newInstance(0, 0),
				DFSConfigKeys.DFS_REPLICATION_DEFAULT)) {

			yarnApplicationFileUploader.registerProvidedLocalResources();

			final Set<String> registeredResources = yarnApplicationFileUploader.getRegisteredLocalResources().keySet();

			assertThat(registeredResources, Matchers.containsInAnyOrder(libJars.keySet().toArray()));
		}
	}

	@Test
	public void testRegisterProvidedLocalResourcesWithDuplication() throws IOException {
		final File flinkLibDir1 = temporaryFolder.newFolder();
		final File flinkLibDir2 = temporaryFolder.newFolder();

		generateFilesInDirectory(flinkLibDir1, getLibJars());
		generateFilesInDirectory(flinkLibDir2, getLibJars());

		final FileSystem fileSystem = FileSystem.get(new YarnConfiguration());
		try {
			assertThrows(
				"Two files with the same filename exist in the shared libs",
				RuntimeException.class,
				() -> YarnApplicationFileUploader.from(
						fileSystem,
						new Path(temporaryFolder.getRoot().toURI()),
						Arrays.asList(new Path(flinkLibDir1.toURI()), new Path(flinkLibDir2.toURI())),
						ApplicationId.newInstance(0, 0),
						DFSConfigKeys.DFS_REPLICATION_DEFAULT));
		} finally {
			IOUtils.closeQuietly(fileSystem);
		}
	}

	private static Map<String, String> getLibJars() {
		final HashMap<String, String> libJars = new HashMap<>(4);
		final String jarContent = "JAR Content";

		libJars.put("flink-dist.jar", jarContent);
		libJars.put("log4j.jar", jarContent);
		libJars.put("flink-table.jar", jarContent);

		return libJars;
	}
}


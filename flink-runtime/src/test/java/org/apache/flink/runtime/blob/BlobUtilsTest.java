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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link BlobUtils}.
 */
public class BlobUtilsTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory} using
	 * {@link BlobServerOptions#STORAGE_DIRECTORY} per default.
	 */
	@Test
	public void testDefaultBlobStorageDirectory() throws IOException {
		Configuration config = new Configuration();
		String blobStorageDir = temporaryFolder.newFolder().getAbsolutePath();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, blobStorageDir);
		config.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			temporaryFolder.newFolder().getAbsolutePath());

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(), startsWith(blobStorageDir));
	}

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory}'s fallback to
	 * {@link ConfigConstants#TASK_MANAGER_TMP_DIR_KEY} with a single temp directory.
	 */
	@Test
	public void testTaskManagerFallbackBlobStorageDirectory1() throws IOException {
		Configuration config = new Configuration();
		String blobStorageDir = temporaryFolder.getRoot().getAbsolutePath();
		config.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, blobStorageDir);

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(), startsWith(blobStorageDir));
	}

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory}'s fallback to
	 * {@link ConfigConstants#TASK_MANAGER_TMP_DIR_KEY} having multiple temp directories.
	 */
	@Test
	public void testTaskManagerFallbackBlobStorageDirectory2a() throws IOException {
		Configuration config = new Configuration();
		String blobStorageDirs = temporaryFolder.newFolder().getAbsolutePath() + "," +
			temporaryFolder.newFolder().getAbsolutePath();
		config.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, blobStorageDirs);

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(), startsWith(temporaryFolder.getRoot().getAbsolutePath()));
	}

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory}'s fallback to
	 * {@link ConfigConstants#TASK_MANAGER_TMP_DIR_KEY} having multiple temp directories.
	 */
	@Test
	public void testTaskManagerFallbackBlobStorageDirectory2b() throws IOException {
		Configuration config = new Configuration();
		String blobStorageDirs = temporaryFolder.newFolder().getAbsolutePath() + File.pathSeparator +
			temporaryFolder.newFolder().getAbsolutePath();
		config.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, blobStorageDirs);

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(), startsWith(temporaryFolder.getRoot().getAbsolutePath()));
	}

	/**
	 * Tests {@link BlobUtils#initLocalStorageDirectory}'s fallback to
	 * {@link ConfigConstants#DEFAULT_TASK_MANAGER_TMP_PATH}.
	 */
	@Test
	public void testTaskManagerFallbackFallbackBlobStorageDirectory1() throws IOException {
		Configuration config = new Configuration();

		File dir = BlobUtils.initLocalStorageDirectory(config);
		assertThat(dir.getAbsolutePath(),
			startsWith(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH));
	}
}

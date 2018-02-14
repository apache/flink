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

package org.apache.flink.runtime.rest;

import org.apache.flink.util.TestLogger;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link FileUploadHandler}.
 */
public class FileUploadHandlerTest extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testCreateUploadDir() throws Exception {
		final File file = temporaryFolder.newFolder();
		final Path testUploadDir = file.toPath().resolve("testUploadDir");
		assertFalse(Files.exists(testUploadDir));
		FileUploadHandler.createUploadDir(testUploadDir);
		assertTrue(Files.exists(testUploadDir));
	}

	@Test
	public void testCreateUploadDirFails() throws Exception {
		final File file = temporaryFolder.newFolder();
		Assume.assumeTrue(file.setWritable(false));

		final Path testUploadDir = file.toPath().resolve("testUploadDir");
		assertFalse(Files.exists(testUploadDir));
		try {
			FileUploadHandler.createUploadDir(testUploadDir);
			fail("Expected exception not thrown.");
		} catch (IOException e) {
		}
	}
}

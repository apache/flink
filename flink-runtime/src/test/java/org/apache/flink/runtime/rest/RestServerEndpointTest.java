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
import org.slf4j.helpers.NOPLogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test cases for the {@link RestServerEndpoint}.
 */
public class RestServerEndpointTest extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that the REST handler URLs are properly sorted.
	 */
	@Test
	public void testRestHandlerUrlSorting() {
		final int numberHandlers = 5;

		final List<String> handlerUrls = new ArrayList<>(numberHandlers);

		handlerUrls.add("/jobs/overview");
		handlerUrls.add("/jobs/:jobid");
		handlerUrls.add("/jobs");
		handlerUrls.add("/:*");
		handlerUrls.add("/jobs/:jobid/config");

		final List<String> expected = new ArrayList<>(numberHandlers);

		expected.add("/jobs");
		expected.add("/jobs/overview");
		expected.add("/jobs/:jobid");
		expected.add("/jobs/:jobid/config");
		expected.add("/:*");

		Collections.sort(handlerUrls, new RestServerEndpoint.RestHandlerUrlComparator.CaseInsensitiveOrderComparator());

		assertEquals(expected, handlerUrls);
	}

	@Test
	public void testCreateUploadDir() throws Exception {
		final File file = temporaryFolder.newFolder();
		final Path testUploadDir = file.toPath().resolve("testUploadDir");
		assertFalse(Files.exists(testUploadDir));
		RestServerEndpoint.createUploadDir(testUploadDir, NOPLogger.NOP_LOGGER);
		assertTrue(Files.exists(testUploadDir));
	}

	@Test
	public void testCreateUploadDirFails() throws Exception {
		final File file = temporaryFolder.newFolder();
		Assume.assumeTrue(file.setWritable(false));

		final Path testUploadDir = file.toPath().resolve("testUploadDir");
		assertFalse(Files.exists(testUploadDir));
		try {
			RestServerEndpoint.createUploadDir(testUploadDir, NOPLogger.NOP_LOGGER);
			fail("Expected exception not thrown.");
		} catch (IOException e) {
		}
	}
}

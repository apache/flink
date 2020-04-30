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

package org.apache.flink.runtime.fs.maprfs;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link MapRFsFactory}.
 */
public class MapRFsFactoryTest extends TestLogger {

	@Test
	public void testMapRFsScheme() throws Exception {
		final Path path = new Path("maprfs:///my/path");

		final FileSystem fs = path.getFileSystem();

		assertEquals(path.toUri().getScheme(), fs.getUri().getScheme());
	}

	@Test
	public void testMapRFsKind() throws Exception {
		final Path path = new Path("maprfs:///my/path");

		final FileSystem fs = path.getFileSystem();

		assertEquals(FileSystemKind.FILE_SYSTEM, fs.getKind());
	}

	@Test
	public void testCreateWithAuthorityNoCldbFails() throws Exception {
		final Path path = new Path("maprfs://localhost:12345/");

		try {
			path.getFileSystem();
			fail("should have failed with an exception");
		}
		catch (IOException e) {
			// expected, because we have no CLDB config available
		}
	}
}

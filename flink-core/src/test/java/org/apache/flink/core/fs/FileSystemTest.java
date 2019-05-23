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

package org.apache.flink.core.fs;

import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.util.WrappingProxy;
import org.apache.flink.util.WrappingProxyUtil;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link FileSystem} base class.
 */
public class FileSystemTest {

	@Test
	public void testGet() throws URISyntaxException, IOException {
		String scheme = "file";

		assertTrue(getFileSystemWithoutSafetyNet(scheme + ":///test/test") instanceof LocalFileSystem);

		try {
			getFileSystemWithoutSafetyNet(scheme + "://test/test");
		} catch (IOException ioe) {
			assertTrue(ioe.getMessage().startsWith("Found local file path with authority '"));
		}

		assertTrue(getFileSystemWithoutSafetyNet(scheme + ":/test/test") instanceof LocalFileSystem);

		assertTrue(getFileSystemWithoutSafetyNet(scheme + ":test/test") instanceof LocalFileSystem);

		assertTrue(getFileSystemWithoutSafetyNet("/test/test") instanceof LocalFileSystem);

		assertTrue(getFileSystemWithoutSafetyNet("test/test") instanceof LocalFileSystem);
	}

	private static FileSystem getFileSystemWithoutSafetyNet(final String uri) throws URISyntaxException, IOException {
		final FileSystem fileSystem = FileSystem.get(new URI(uri));

		if (fileSystem instanceof WrappingProxy) {
			//noinspection unchecked
			return WrappingProxyUtil.stripProxy((WrappingProxy<FileSystem>) fileSystem);
		}

		return fileSystem;
	}

}

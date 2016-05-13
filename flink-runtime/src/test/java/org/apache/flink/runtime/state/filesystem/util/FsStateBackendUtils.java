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

package org.apache.flink.runtime.state.filesystem.util;

import org.apache.commons.io.FileUtils;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FsStateBackendUtils {
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	public static void ensureLocalFileDeleted(Path path) {
		URI uri = path.toUri();
		if ("file".equals(uri.getScheme())) {
			File file = new File(uri.getPath());
			assertFalse("file not properly deleted", file.exists());
		}
		else {
			throw new IllegalArgumentException("not a local path");
		}
	}

	public static void deleteDirectorySilently(File dir) {
		try {
			FileUtils.deleteDirectory(dir);
		}
		catch (IOException ignored) {}
	}

	public static boolean isDirectoryEmpty(File directory) {
		if (!directory.exists()) {
			return true;
		}
		String[] nested = directory.list();
		return nested == null || nested.length == 0;
	}

	public static String localFileUri(File path) {
		return path.toURI().toString();
	}

	public static void validateBytesInStream(InputStream is, byte[] data) throws IOException {
		byte[] holder = new byte[data.length];

		int pos = 0;
		int read;
		while (pos < holder.length && (read = is.read(holder, pos, holder.length - pos)) != -1) {
			pos += read;
		}

		assertEquals("not enough data", holder.length, pos);
		assertEquals("too much data", -1, is.read());
		assertArrayEquals("wrong data", data, holder);
		is.close();
	}
}

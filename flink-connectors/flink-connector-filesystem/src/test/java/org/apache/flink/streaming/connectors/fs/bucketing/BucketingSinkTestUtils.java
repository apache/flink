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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Test utilities for bucketing sinks.
 */
public class BucketingSinkTestUtils {

	public static final String PART_PREFIX = "part";
	public static final String PENDING_SUFFIX = ".pending";
	public static final String IN_PROGRESS_SUFFIX = ".in-progress";
	public static final String VALID_LENGTH_SUFFIX = ".valid";

	/**
	 * Verifies the correct number of written files and reasonable length files.
	 */
	public static void checkLocalFs(File outDir, int inprogress, int pending, int completed, int valid) throws IOException {
		int inProg = 0;
		int pend = 0;
		int compl = 0;
		int val = 0;

		for (File file: FileUtils.listFiles(outDir, null, true)) {
			if (file.getAbsolutePath().endsWith("crc")) {
				continue;
			}
			String path = file.getPath();
			if (path.endsWith(IN_PROGRESS_SUFFIX)) {
				inProg++;
			} else if (path.endsWith(PENDING_SUFFIX)) {
				pend++;
			} else if (path.endsWith(VALID_LENGTH_SUFFIX)) {
				// check that content of length file is valid
				try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
					final long validLength = Long.valueOf(dis.readUTF());
					final String truncated = path.substring(0, path.length() - VALID_LENGTH_SUFFIX.length());
					Assert.assertTrue("Mismatch between valid length and file size.",
						FileUtils.sizeOf(new File(truncated)) >= validLength);
				}
				val++;
			} else if (path.contains(PART_PREFIX)) {
				compl++;
			}
		}

		Assert.assertEquals(inprogress, inProg);
		Assert.assertEquals(pending, pend);
		Assert.assertEquals(completed, compl);
		// check length file in case truncating is not supported
		try {
			RawLocalFileSystem.class.getMethod("truncate", Path.class, long.class);
		} catch (NoSuchMethodException e) {
			Assert.assertEquals(valid, val);
		}
	}
}

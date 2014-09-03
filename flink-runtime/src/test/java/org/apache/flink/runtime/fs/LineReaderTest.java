/**
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


package org.apache.flink.runtime.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.PrintWriter;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.Test;

/**
 * This class tests the functionality of the LineReader class using a local filesystem.
 * 
 */

public class LineReaderTest {

	/**
	 * This test tests the LineReader. So far only under usual conditions.
	 */
	@Test
	public void testLineReader() {
		final File testfile = new File(CommonTestUtils.getTempDir() + File.separator
			+ CommonTestUtils.getRandomFilename());
		final Path pathtotestfile = new Path(testfile.toURI().getPath());

		try {
			PrintWriter pw = new PrintWriter(testfile, "UTF8");

			for (int i = 0; i < 100; i++) {
				pw.append("line\n");
			}
			pw.close();

			LocalFileSystem lfs = new LocalFileSystem();
			FSDataInputStream fis = lfs.open(pathtotestfile);

			// first, we test under "usual" conditions
			final LineReader lr = new LineReader(fis, 0, testfile.length(), 256);

			byte[] buffer;
			int linecount = 0;
			while ((buffer = lr.readLine()) != null) {
				assertEquals(new String(buffer, "UTF8"), "line");
				linecount++;
			}
			assertEquals(linecount, 100);

			// the linereader can not handle situations with larger length than the total file...

		} catch (Exception e) {
			fail(e.toString());
			e.printStackTrace();
		} finally {
			testfile.delete();
		}

	}

}

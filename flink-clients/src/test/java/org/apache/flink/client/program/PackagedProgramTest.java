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

package org.apache.flink.client.program;

import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.configuration.ConfigConstants;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Tests for the {@link PackagedProgramTest}.
 */
public class PackagedProgramTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testGetPreviewPlan() {
		try {
			PackagedProgram prog = new PackagedProgram(new File(CliFrontendTestUtils.getTestJarPath()));

			final PrintStream out = System.out;
			final PrintStream err = System.err;
			try {
				System.setOut(new PrintStream(new NullOutputStream()));
				System.setErr(new PrintStream(new NullOutputStream()));

				Assert.assertNotNull(prog.getPreviewPlan());
			}
			finally {
				System.setOut(out);
				System.setErr(err);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test is erroneous: " + e.getMessage());
		}
	}

	@Test
	public void testExtractContainedLibraries() throws Exception {
		String s = "testExtractContainedLibraries";
		byte[] nestedJarContent = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
		File fakeJar = temporaryFolder.newFile("test.jar");
		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(fakeJar))) {
			ZipEntry entry = new ZipEntry("lib/internalTest.jar");
			zos.putNextEntry(entry);
			zos.write(nestedJarContent);
			zos.closeEntry();
		}

		final List<File> files = PackagedProgram.extractContainedLibraries(fakeJar.toURI().toURL());
		Assert.assertEquals(1, files.size());
		Assert.assertArrayEquals(nestedJarContent, Files.readAllBytes(files.iterator().next().toPath()));
	}

	private static final class NullOutputStream extends java.io.OutputStream {
		@Override
		public void write(int b) {}
	}
}

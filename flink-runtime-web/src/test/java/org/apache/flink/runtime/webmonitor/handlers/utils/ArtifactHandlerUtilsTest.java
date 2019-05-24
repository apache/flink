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

package org.apache.flink.runtime.webmonitor.handlers.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ArtifactHandlerUtils}.
 */
public class ArtifactHandlerUtilsTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testTokenizeNonQuoted() {
		final List<String> arguments = ArtifactHandlerUtils.tokenizeArguments("--foo bar");
		assertThat(arguments.get(0), equalTo("--foo"));
		assertThat(arguments.get(1), equalTo("bar"));
	}

	@Test
	public void testTokenizeSingleQuoted() {
		final List<String> arguments = ArtifactHandlerUtils.tokenizeArguments("--foo 'bar baz '");
		assertThat(arguments.get(0), equalTo("--foo"));
		assertThat(arguments.get(1), equalTo("bar baz "));
	}

	@Test
	public void testTokenizeDoubleQuoted() {
		final List<String> arguments = ArtifactHandlerUtils.tokenizeArguments("--name \"K. Bote \"");
		assertThat(arguments.get(0), equalTo("--name"));
		assertThat(arguments.get(1), equalTo("K. Bote "));
	}

	@Test
	public void testExtractContainedLibraries() throws Exception {
		byte[] nestedJarContent = "testExtractContainedLibraries_jar".getBytes(ConfigConstants.DEFAULT_CHARSET);
		byte[] nestedPyContent1 = "testExtractContainedLibraries_py".getBytes(ConfigConstants.DEFAULT_CHARSET);
		byte[] nestedPyContent2 = "testExtractContainedLibraries_zip".getBytes(ConfigConstants.DEFAULT_CHARSET);
		File fakeZip = temporaryFolder.newFile("test.zip");
		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(fakeZip))) {
			ZipEntry entry = new ZipEntry("lib/internalTest.jar");
			zos.putNextEntry(entry);
			zos.write(nestedJarContent);
			zos.closeEntry();

			entry = new ZipEntry("lib/internalTest.py");
			zos.putNextEntry(entry);
			zos.write(nestedPyContent1);
			zos.closeEntry();

			entry = new ZipEntry("lib/internalTest.zip");
			zos.putNextEntry(entry);
			zos.write(nestedPyContent2);
			zos.closeEntry();

			entry = new ZipEntry("internalTest.zip");
			zos.putNextEntry(entry);
			zos.write(nestedPyContent2);
			zos.closeEntry();
		}

		final Tuple2<File, List<File>> containedLibraries =
			ArtifactHandlerUtils.extractContainedLibraries(fakeZip.toURI());
		Assert.assertArrayEquals(nestedJarContent, Files.readAllBytes(containedLibraries.f0.toPath()));
		Assert.assertEquals(2, containedLibraries.f1.size());

		Set<Object> containedPyFiles =
			new HashSet<>(Arrays.asList(containedLibraries.f1.stream().map(File::getName).toArray()));
		Assert.assertTrue(containedPyFiles.stream().anyMatch(f -> f.toString().endsWith("internalTest.py")));
		Assert.assertTrue(containedPyFiles.stream().anyMatch(f -> f.toString().endsWith("internalTest.zip")));
	}
}

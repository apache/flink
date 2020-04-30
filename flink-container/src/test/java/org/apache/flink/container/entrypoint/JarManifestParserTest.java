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

package org.apache.flink.container.entrypoint;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.container.entrypoint.JarManifestParser.JarFileWithEntryClass;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for JAR file manifest parsing.
 */
public class JarManifestParserTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testFindEntryClassNoEntry() throws IOException {
		File jarFile = createJarFileWithManifest(ImmutableMap.of());

		Optional<String> entry = JarManifestParser.findEntryClass(jarFile);

		assertFalse(entry.isPresent());
	}

	@Test
	public void testFindEntryClassAssemblerClass() throws IOException {
		File jarFile = createJarFileWithManifest(ImmutableMap.of(
			PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS, "AssemblerClass"));

		Optional<String> entry = JarManifestParser.findEntryClass(jarFile);

		assertTrue(entry.isPresent());
		assertThat(entry.get(), is(equalTo("AssemblerClass")));
	}

	@Test
	public void testFindEntryClassMainClass() throws IOException {
		File jarFile = createJarFileWithManifest(ImmutableMap.of(
			PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS, "MainClass"));

		Optional<String> entry = JarManifestParser.findEntryClass(jarFile);

		assertTrue(entry.isPresent());
		assertThat(entry.get(), is(equalTo("MainClass")));
	}

	@Test
	public void testFindEntryClassAssemblerClassAndMainClass() throws IOException {
		// We want the assembler class entry to have precedence over main class
		File jarFile = createJarFileWithManifest(ImmutableMap.of(
			PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS, "AssemblerClass",
			PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS, "MainClass"));

		Optional<String> entry = JarManifestParser.findEntryClass(jarFile);

		assertTrue(entry.isPresent());
		assertThat(entry.get(), is(equalTo("AssemblerClass")));
	}

	@Test
	public void testFindEntryClassWithTestJobJar() throws IOException {
		File jarFile = TestJob.getTestJobJar();

		Optional<String> entryClass = JarManifestParser.findEntryClass(jarFile);

		assertTrue(entryClass.isPresent());
		assertThat(entryClass.get(), is(equalTo(TestJob.class.getCanonicalName())));
	}

	@Test(expected = NoSuchElementException.class)
	public void testFindOnlyEntryClassEmptyArgument() throws IOException {
		JarManifestParser.findOnlyEntryClass(Collections.emptyList());
	}

	@Test(expected = NoSuchElementException.class)
	public void testFindOnlyEntryClassSingleJarWithNoManifest() throws IOException {
		File jarWithNoManifest = createJarFileWithManifest(ImmutableMap.of());
		JarManifestParser.findOnlyEntryClass(ImmutableList.of(jarWithNoManifest));
	}

	@Test
	public void testFindOnlyEntryClassSingleJar() throws IOException {
		File jarFile = TestJob.getTestJobJar();

		JarFileWithEntryClass jarFileWithEntryClass = JarManifestParser.findOnlyEntryClass(ImmutableList.of(jarFile));

		assertThat(jarFileWithEntryClass.getEntryClass(), is(equalTo(TestJob.class.getCanonicalName())));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFindOnlyEntryClassMultipleJarsWithMultipleManifestEntries() throws IOException {
		File jarFile = TestJob.getTestJobJar();

		JarManifestParser.findOnlyEntryClass(ImmutableList.of(jarFile, jarFile, jarFile));
	}

	@Test
	public void testFindOnlyEntryClassMultipleJarsWithSingleManifestEntry() throws IOException {
		File jarWithNoManifest = createJarFileWithManifest(ImmutableMap.of());
		File jarFile = TestJob.getTestJobJar();

		JarFileWithEntryClass jarFileWithEntryClass = JarManifestParser
			.findOnlyEntryClass(ImmutableList.of(jarWithNoManifest, jarFile));

		assertThat(jarFileWithEntryClass.getEntryClass(), is(equalTo(TestJob.class.getCanonicalName())));
	}

	private File createJarFileWithManifest(Map<String, String> manifest) throws IOException {
		final File jarFile = temporaryFolder.newFile();
		try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(jarFile)));
				PrintWriter pw = new PrintWriter(zos)) {
			zos.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
			manifest.forEach((key, value) -> pw.println(String.format("%s: %s", key, value)));
		}
		return jarFile;
	}

}

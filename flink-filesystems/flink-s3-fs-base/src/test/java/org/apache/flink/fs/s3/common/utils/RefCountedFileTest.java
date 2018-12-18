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

package org.apache.flink.fs.s3.common.utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Tests for the {@link RefCountedFile}.
 */
public class RefCountedFileTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void releaseToZeroRefCounterShouldDeleteTheFile() throws IOException {
		final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
		final OutputStream out = Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

		RefCountedFile fileUnderTest = RefCountedFile.newFile(newFile, out);
		verifyTheFileIsStillThere();

		fileUnderTest.release();

		try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
			Assert.assertEquals(0L, files.count());
		}
	}

	@Test
	public void retainsShouldRequirePlusOneReleasesToDeleteTheFile() throws IOException {
		final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
		final OutputStream out = Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

		// the reference counter always starts with 1 (not 0). This is why we need +1 releases
		RefCountedFile fileUnderTest = RefCountedFile.newFile(newFile, out);
		verifyTheFileIsStillThere();

		fileUnderTest.retain();
		fileUnderTest.retain();

		Assert.assertEquals(3, fileUnderTest.getReferenceCounter());

		fileUnderTest.release();
		Assert.assertEquals(2, fileUnderTest.getReferenceCounter());
		verifyTheFileIsStillThere();

		fileUnderTest.release();
		Assert.assertEquals(1, fileUnderTest.getReferenceCounter());
		verifyTheFileIsStillThere();

		fileUnderTest.release();
		// the file is deleted now
		try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
			Assert.assertEquals(0L, files.count());
		}
	}

	@Test
	public void writeShouldSucceed() throws IOException {
		byte[] content = bytesOf("hello world");

		final RefCountedFile fileUnderTest = getClosedRefCountedFileWithContent(content);
		long fileLength = fileUnderTest.getLength();

		Assert.assertEquals(content.length, fileLength);
	}

	@Test
	public void closeShouldNotReleaseReference() throws IOException {
		getClosedRefCountedFileWithContent("hello world");
		verifyTheFileIsStillThere();
	}

	@Test(expected = IOException.class)
	public void writeAfterCloseShouldThrowException() throws IOException {
		final RefCountedFile fileUnderTest = getClosedRefCountedFileWithContent("hello world");
		byte[] content = bytesOf("Hello Again");
		fileUnderTest.write(content, 0, content.length);
	}

	@Test(expected = IOException.class)
	public void flushAfterCloseShouldThrowException() throws IOException {
		final RefCountedFile fileUnderTest = getClosedRefCountedFileWithContent("hello world");
		fileUnderTest.flush();
	}

	// ------------------------------------- Utilities -------------------------------------

	private void verifyTheFileIsStillThere() throws IOException {
		try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
			Assert.assertEquals(1L, files.count());
		}
	}

	private RefCountedFile getClosedRefCountedFileWithContent(String content) throws IOException {
		return getClosedRefCountedFileWithContent(bytesOf(content));
	}

	private RefCountedFile getClosedRefCountedFileWithContent(byte[] content) throws IOException {
		final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
		final OutputStream out = Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

		final RefCountedFile fileUnderTest = RefCountedFile.newFile(newFile, out);

		fileUnderTest.write(content, 0, content.length);

		fileUnderTest.closeStream();
		return fileUnderTest;
	}

	private static byte[] bytesOf(String str) {
		return str.getBytes(StandardCharsets.UTF_8);
	}
}

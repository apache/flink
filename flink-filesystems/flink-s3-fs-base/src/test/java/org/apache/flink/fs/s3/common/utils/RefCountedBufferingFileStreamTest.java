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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

/**
 * Tests for the {@link RefCountedBufferingFileStream}.
 */
public class RefCountedBufferingFileStreamTest {

	private static final int BUFFER_SIZE = 10;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testSmallWritesGoToBuffer() throws IOException {
		RefCountedBufferingFileStream stream = getStreamToTest();

		final byte[] contentToWrite = bytesOf("hello");
		stream.write(contentToWrite);

		Assert.assertEquals(contentToWrite.length, stream.getPositionInBuffer());
		Assert.assertEquals(contentToWrite.length, stream.getPos());

		stream.close();
		stream.release();
	}

	@Test(expected = IOException.class)
	public void testExceptionWhenWritingToClosedFile() throws IOException {
		RefCountedBufferingFileStream stream = getStreamToTest();

		final byte[] contentToWrite = bytesOf("hello");
		stream.write(contentToWrite);

		Assert.assertEquals(contentToWrite.length, stream.getPositionInBuffer());
		Assert.assertEquals(contentToWrite.length, stream.getPos());

		stream.close();

		stream.write(contentToWrite);
	}

	@Test
	public void testBigWritesGoToFile() throws IOException {
		RefCountedBufferingFileStream stream = getStreamToTest();

		final byte[] contentToWrite = bytesOf("hello big world");
		stream.write(contentToWrite);

		Assert.assertEquals(0, stream.getPositionInBuffer());
		Assert.assertEquals(contentToWrite.length, stream.getPos());

		stream.close();
		stream.release();
	}

	@Test
	public void testSpillingWhenBufferGetsFull() throws IOException {
		RefCountedBufferingFileStream stream = getStreamToTest();

		final byte[] firstContentToWrite = bytesOf("hello");
		stream.write(firstContentToWrite);

		Assert.assertEquals(firstContentToWrite.length, stream.getPositionInBuffer());
		Assert.assertEquals(firstContentToWrite.length, stream.getPos());

		final byte[] secondContentToWrite = bytesOf(" world!");
		stream.write(secondContentToWrite);

		Assert.assertEquals(secondContentToWrite.length, stream.getPositionInBuffer());
		Assert.assertEquals(firstContentToWrite.length + secondContentToWrite.length, stream.getPos());

		stream.close();
		stream.release();
	}

	@Test
	public void testFlush() throws IOException {
		RefCountedBufferingFileStream stream = getStreamToTest();

		final byte[] contentToWrite = bytesOf("hello");
		stream.write(contentToWrite);

		Assert.assertEquals(contentToWrite.length, stream.getPositionInBuffer());
		Assert.assertEquals(contentToWrite.length, stream.getPos());

		stream.flush();

		Assert.assertEquals(0, stream.getPositionInBuffer());
		Assert.assertEquals(contentToWrite.length, stream.getPos());

		final byte[] contentRead = new byte[contentToWrite.length];
		new FileInputStream(stream.getInputFile()).read(contentRead, 0, contentRead.length);
		Assert.assertTrue(Arrays.equals(contentToWrite, contentRead));

		stream.release();
	}

	// ---------------------------- Utility Classes ----------------------------

	private RefCountedBufferingFileStream getStreamToTest() throws IOException {
		return new RefCountedBufferingFileStream(getRefCountedFileWithContent(), BUFFER_SIZE);
	}

	private RefCountedFile getRefCountedFileWithContent() throws IOException {
		final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
		final OutputStream out = Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

		return RefCountedFile.newFile(newFile, out);
	}

	private static byte[] bytesOf(String str) {
		return str.getBytes(StandardCharsets.UTF_8);
	}
}

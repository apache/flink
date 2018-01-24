/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.memory;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Suite of tests for the {@link ByteArrayPrependedInputStream}.
 */
public class ByteArrayPrependedInputStreamTest {

	private static final byte[] TEST_PREPEND_BUFFER = new byte[] {12, 52, 127, 121};
	private static final byte[] TEST_INPUT_STREAM_BYTES = new byte[] {51, 45, 89, 45, 100, 96};

	@Test
	public void testRead() throws IOException {
		testRead(0);
	}

	@Test
	public void testReadWithInputStreamAtMidPosition() throws IOException {
		testRead(3);
	}

	private void testRead(int numBytesToPreReadFromInputStream) throws IOException {
		final InputStream testInputStream = new ByteArrayInputStream(TEST_INPUT_STREAM_BYTES);

		// read the input stream first
		testInputStream.read(new byte[numBytesToPreReadFromInputStream]);

		final ByteArrayPrependedInputStream stream = new ByteArrayPrependedInputStream(TEST_PREPEND_BUFFER, testInputStream);

		for (int i = 0; i < TEST_PREPEND_BUFFER.length; i++) {
			Assert.assertEquals(TEST_PREPEND_BUFFER[i], stream.read());
		}

		for (int i = numBytesToPreReadFromInputStream; i < TEST_INPUT_STREAM_BYTES.length; i++) {
			Assert.assertEquals(TEST_INPUT_STREAM_BYTES[i], stream.read());
		}

		Assert.assertEquals(-1, stream.read());
	}

	@Test
	public void testReadIntoBuffer() throws IOException {
		testReadIntoBuffer(0);
	}

	@Test
	public void testReadIntoBufferWithInputStreamAtMidPosition() throws IOException {
		testReadIntoBuffer(2);
	}

	private void testReadIntoBuffer(int numBytesToPreReadFromInputStream) throws IOException {
		final InputStream testInputStream = new ByteArrayInputStream(TEST_INPUT_STREAM_BYTES);

		// read the input stream first
		testInputStream.read(new byte[numBytesToPreReadFromInputStream]);

		int numRemainingBytesInInputStream = TEST_INPUT_STREAM_BYTES.length - numBytesToPreReadFromInputStream;

		final ByteArrayPrependedInputStream stream = new ByteArrayPrependedInputStream(TEST_PREPEND_BUFFER, testInputStream);

		int numBytesToSplitInPrependBuffer = 2;
		int numBytesToSplitInInputStream = 3;
		byte[] buffer1 = new byte[TEST_PREPEND_BUFFER.length - numBytesToSplitInPrependBuffer]; // should only be filled with the prepend buffer bytes
		byte[] buffer2 = new byte[numBytesToSplitInPrependBuffer + numBytesToSplitInInputStream]; // should contain bytes from the prepend buffer and input stream
		byte[] buffer3 = new byte[numRemainingBytesInInputStream - numBytesToSplitInInputStream]; // should contain remaining bytes from the input stream

		stream.read(buffer1);
		stream.read(buffer2);
		stream.read(buffer3);

		for (int i = 0; i < TEST_PREPEND_BUFFER.length - numBytesToSplitInPrependBuffer; i++) {
			Assert.assertEquals(TEST_PREPEND_BUFFER[i], buffer1[i]);
		}

		for (int i = 0; i < numBytesToSplitInPrependBuffer; i++) {
			Assert.assertEquals(TEST_PREPEND_BUFFER[i + numBytesToSplitInPrependBuffer], buffer2[i]);
		}

		for (int i = 0; i < numBytesToSplitInInputStream; i++) {
			Assert.assertEquals(TEST_INPUT_STREAM_BYTES[i + numBytesToPreReadFromInputStream], buffer2[i + numBytesToSplitInPrependBuffer]);
		}

		for (int i = 0; i < numRemainingBytesInInputStream - numBytesToSplitInInputStream; i++) {
			Assert.assertEquals(TEST_INPUT_STREAM_BYTES[i + numBytesToPreReadFromInputStream + numBytesToSplitInInputStream], buffer3[i]);
		}

		Assert.assertEquals(-1, stream.read());
	}

	@Test
	public void testSkipWithinPrependBuffer() throws IOException {
		testSkip(TEST_PREPEND_BUFFER.length - 2, TEST_PREPEND_BUFFER[2], 0);
	}

	@Test
	public void testSkipPrependBufferCompletely() throws IOException {
		testSkip(TEST_PREPEND_BUFFER.length, TEST_INPUT_STREAM_BYTES[0], 0);
		testSkip(TEST_PREPEND_BUFFER.length, TEST_INPUT_STREAM_BYTES[2], 2);
	}

	@Test
	public void testSkipToInputStream() throws IOException {
		testSkip(
			TEST_PREPEND_BUFFER.length + (TEST_INPUT_STREAM_BYTES.length - 3),
			TEST_INPUT_STREAM_BYTES[3],
			0);

		int numBytesToPreReadFromInputStream = 3;
		testSkip(
			TEST_PREPEND_BUFFER.length + 2,
			TEST_INPUT_STREAM_BYTES[numBytesToPreReadFromInputStream + 2],
			numBytesToPreReadFromInputStream);
	}

	private void testSkip(int numBytesToSkip, int expectedRead, int numBytesToPreReadFromInputStream) throws IOException {
		final InputStream testInputStream = new ByteArrayInputStream(TEST_INPUT_STREAM_BYTES);
		testInputStream.read(new byte[numBytesToPreReadFromInputStream]);

		final ByteArrayPrependedInputStream stream = new ByteArrayPrependedInputStream(TEST_PREPEND_BUFFER, testInputStream);

		stream.skip(numBytesToSkip);
		Assert.assertEquals(expectedRead, stream.read());
	}

	@Test
	public void testMarkAndResetWithinPrependBuffer() throws IOException {
		// mark within the prepend buffer; after marking, read within the prepend buffer
		testMarkAndReset(2, 1, TEST_PREPEND_BUFFER[2], 0);

		// mark within the prepend buffer; after marking, read to the end of the prepend buffer
		testMarkAndReset(2, TEST_PREPEND_BUFFER.length - 2, TEST_PREPEND_BUFFER[2], 0);

		// mark within the prepend buffer; after marking, read across to the input stream
		testMarkAndReset(2, TEST_PREPEND_BUFFER.length, TEST_PREPEND_BUFFER[2], 0);

		// repeat above cases, but with the input stream pre-read
		testMarkAndReset(2, 1, TEST_PREPEND_BUFFER[2], 2);
		testMarkAndReset(2, TEST_PREPEND_BUFFER.length - 2, TEST_PREPEND_BUFFER[2], 2);
		testMarkAndReset(2, TEST_PREPEND_BUFFER.length, TEST_PREPEND_BUFFER[2], 2);
	}

	@Test
	public void testMarkAndResetAtEndOfPrependBuffer() throws IOException {
		// mark at the end of the prepend buffer; after marking, do not read anything
		testMarkAndReset(TEST_PREPEND_BUFFER.length, 0, TEST_INPUT_STREAM_BYTES[0], 0);

		// mark at the end of the prepend buffer; after marking, read a few bytes from the input stream
		testMarkAndReset(TEST_PREPEND_BUFFER.length, 2, TEST_INPUT_STREAM_BYTES[0], 0);

		// repeat above cases, but with the input stream pre-read
		int numBytesToPreReadFromInputStream = 3;
		testMarkAndReset(TEST_PREPEND_BUFFER.length, 0, TEST_INPUT_STREAM_BYTES[numBytesToPreReadFromInputStream], numBytesToPreReadFromInputStream);
		testMarkAndReset(TEST_PREPEND_BUFFER.length, 2, TEST_INPUT_STREAM_BYTES[numBytesToPreReadFromInputStream], numBytesToPreReadFromInputStream);
	}

	@Test
	public void testMarkAndResetWithinInputStream() throws IOException {
		testMarkAndReset(TEST_PREPEND_BUFFER.length + 2, 2, TEST_INPUT_STREAM_BYTES[2], 0);

		int numBytesToPreReadFromInputStream = 2;
		testMarkAndReset(TEST_PREPEND_BUFFER.length + 2, 2, TEST_INPUT_STREAM_BYTES[numBytesToPreReadFromInputStream + 2], numBytesToPreReadFromInputStream);
	}

	private void testMarkAndReset(
			int numBytesToReadBeforeMark,
			int numBytesToReadAfterMark,
			int expectedReadAfterReset,
			int numBytesToPreReadFromInputStream) throws IOException {

		final InputStream testInputStream = new ByteArrayInputStream(TEST_INPUT_STREAM_BYTES);
		testInputStream.read(new byte[numBytesToPreReadFromInputStream]);

		final ByteArrayPrependedInputStream stream = new ByteArrayPrependedInputStream(TEST_PREPEND_BUFFER, testInputStream);

		stream.read(new byte[numBytesToReadBeforeMark]);
		stream.mark(-1);
		stream.read(new byte[numBytesToReadAfterMark]);
		stream.reset();
		Assert.assertEquals(expectedReadAfterReset, stream.read());
	}
}

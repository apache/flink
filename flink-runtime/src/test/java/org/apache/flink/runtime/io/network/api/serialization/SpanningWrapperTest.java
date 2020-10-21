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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.core.memory.MemorySegmentFactory.wrap;
import static org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer.LENGTH_BYTES;
import static org.junit.Assert.assertArrayEquals;

/**
 * {@link SpanningWrapper} test.
 */
public class SpanningWrapperTest {

	private static final Random random = new Random();

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testLargeUnconsumedSegment() throws Exception {
		int recordLen = 100;
		int firstChunk = (int) (recordLen * .9);
		int spillingThreshold = (int) (firstChunk * .9);

		byte[] record1 = recordBytes(recordLen);
		byte[] record2 = recordBytes(recordLen * 2);

		File canNotEecutableFile = folder.newFolder();
		canNotEecutableFile.setExecutable(false);
		// Always pick 'canNotEecutableFile' first as the Spilling Channel TmpDir. Thus trigger an IOException.
		SpanningWrapper spanningWrapper = new SpanningWrapper(new String[]{folder.newFolder().getAbsolutePath(), canNotEecutableFile.getAbsolutePath() + File.separator + "pathdonotexit"}, spillingThreshold, recordLen);
		spanningWrapper.transferFrom(wrapNonSpanning(record1, firstChunk), recordLen);
		spanningWrapper.addNextChunkFromMemorySegment(wrap(record1), firstChunk, recordLen - firstChunk + LENGTH_BYTES);
		spanningWrapper.addNextChunkFromMemorySegment(wrap(record2), 0, record2.length);

		CloseableIterator<Buffer> unconsumedSegment = spanningWrapper.getUnconsumedSegment();

		spanningWrapper.getInputView().readFully(new byte[recordLen], 0, recordLen); // read out from file
		spanningWrapper.transferLeftOverTo(new NonSpanningWrapper()); // clear any leftover
		spanningWrapper.transferFrom(wrapNonSpanning(recordBytes(recordLen), recordLen), recordLen); // overwrite with new data

		canNotEecutableFile.setExecutable(true);

		assertArrayEquals(concat(record1, record2), toByteArray(unconsumedSegment));
	}

	private byte[] recordBytes(int recordLen) {
		byte[] inputData = randomBytes(recordLen + LENGTH_BYTES);
		for (int i = 0; i < Integer.BYTES; i++) {
			inputData[Integer.BYTES - i - 1] = (byte) (recordLen >>> i * 8);
		}
		return inputData;
	}

	private NonSpanningWrapper wrapNonSpanning(byte[] bytes, int len) {
		NonSpanningWrapper nonSpanningWrapper = new NonSpanningWrapper();
		MemorySegment segment = wrap(bytes);
		nonSpanningWrapper.initializeFromMemorySegment(segment, 0, len);
		nonSpanningWrapper.readInt(); // emulate read length performed in getNextRecord to move position
		return nonSpanningWrapper;
	}

	private byte[] toByteArray(CloseableIterator<Buffer> unconsumed) {
		final List<Buffer> buffers = new ArrayList<>();
		try {
			unconsumed.forEachRemaining(buffers::add);
			byte[] result = new byte[buffers.stream().mapToInt(Buffer::readableBytes).sum()];
			int offset = 0;
			for (Buffer buffer : buffers) {
				int len = buffer.readableBytes();
				buffer.getNioBuffer(0, len).get(result, offset, len);
				offset += len;
			}
			return result;
		} finally {
			buffers.forEach(Buffer::recycleBuffer);
		}
	}

	private byte[] randomBytes(int length) {
		byte[] inputData = new byte[length];
		random.nextBytes(inputData);
		return inputData;
	}

	private byte[] concat(byte[] input1, byte[] input2) {
		byte[] expected = new byte[input1.length + input2.length];
		System.arraycopy(input1, 0, expected, 0, input1.length);
		System.arraycopy(input2, 0, expected, input1.length, input2.length);
		return expected;
	}

}

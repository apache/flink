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

package org.apache.flink.api.common.io.blockcompression;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader.CompressionMethod;
import static org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader.CompressionMethod.BZIP2;
import static org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader.CompressionMethod.GZIP;
import static org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader.CompressionMethod.LZ4;
import static org.junit.Assert.assertEquals;

public class BlockCompressionTest {

	@Test
	public void testLz4() throws IOException {
		runArrayTest(LZ4, 32768);
		runArrayTest(LZ4, 16);

		runByteBufferTest(LZ4, false, 32768);
		runByteBufferTest(LZ4, false, 16);
		runByteBufferTest(LZ4, true, 32768);
		runByteBufferTest(LZ4, true, 16);
	}

	@Test
	public void testBzip2() throws IOException {
		runArrayTest(BZIP2, 32768);
		runArrayTest(BZIP2, 16);

		runByteBufferTest(BZIP2, false, 32768);
		runByteBufferTest(BZIP2, false, 16);
		runByteBufferTest(BZIP2, true, 32768);
		runByteBufferTest(BZIP2, true, 16);
	}

	@Test
	public void testGzip() throws IOException {
		runArrayTest(GZIP, 32768);
		runArrayTest(GZIP, 16);

		runByteBufferTest(GZIP, false, 32768);
		runByteBufferTest(GZIP, false, 16);
		runByteBufferTest(GZIP, true, 32768);
		runByteBufferTest(GZIP, true, 16);
	}

	private void runArrayTest(CompressionMethod method, int originalLen) throws IOException {
		BlockCompressionFactory blockCompressionFactory = BlockCompressionFactoryLoader.createBlockCompressionFactory(
			method.name(), new Configuration());
		AbstractBlockCompressor compressor = blockCompressionFactory.getCompressor();
		AbstractBlockDecompressor decompressor = blockCompressionFactory.getDecompressor();

		int originalOff = 64;
		byte[] data = new byte[originalOff + originalLen];
		for (int i = 0; i < originalLen; i++) {
			data[originalOff + i] = (byte) i;
		}

		int compressedOff = 32;
		byte[] compressedData = new byte[compressedOff + compressor.getMaxCompressedSize(originalLen)];
		int compressedLen = compressor.compress(data, originalOff, originalLen, compressedData, compressedOff);

		int decompressedOff = 16;
		byte[] decompressedData = new byte[decompressedOff + originalLen];
		decompressor.decompress(compressedData, compressedOff, compressedLen, decompressedData, decompressedOff);

		for (int i = 0; i < originalLen; i++) {
			assertEquals(data[originalOff + i], decompressedData[decompressedOff + i]);
		}
	}

	private void runByteBufferTest(
		BlockCompressionFactoryLoader.CompressionMethod method,
		boolean isDirect, int originalLen) throws IOException {
		BlockCompressionFactory blockCompressionFactory = BlockCompressionFactoryLoader.createBlockCompressionFactory(
			method.name(), new Configuration());
		AbstractBlockCompressor compressor = blockCompressionFactory.getCompressor();
		AbstractBlockDecompressor decompressor = blockCompressionFactory.getDecompressor();

		int originalOff = 64;
		ByteBuffer data;
		if (isDirect) {
			data = ByteBuffer.allocateDirect(originalOff + originalLen);
		} else {
			data = ByteBuffer.allocate(originalOff + originalLen);
		}

		// Useless data
		for (int i = 0; i < originalOff; i++) {
			data.put((byte) 0x5a);
		}

		for (int i = 0; i < originalLen; i++) {
			data.put((byte) i);
		}
		data.flip();

		ByteBuffer compressedData;
		int maxCompressedLen = compressor.getMaxCompressedSize(originalLen);
		if (isDirect) {
			compressedData = ByteBuffer.allocateDirect(maxCompressedLen);
		} else {
			compressedData = ByteBuffer.allocate(maxCompressedLen);
		}
		int compressedLen = compressor.compress(data, originalOff, originalLen, compressedData, 0);
		assertEquals(compressedLen, compressedData.position());
		compressedData.flip();

		int compressedOff = 32;
		ByteBuffer copiedCompressedData;
		if (isDirect) {
			copiedCompressedData = ByteBuffer.allocateDirect(compressedOff + compressedLen);
		} else {
			copiedCompressedData = ByteBuffer.allocate(compressedOff + compressedLen);
		}

		// Useless data
		for (int i = 0; i < compressedOff; i++) {
			copiedCompressedData.put((byte) 0x5a);
		}

		byte[] compressedByteArray = new byte[compressedLen];
		compressedData.get(compressedByteArray, 0, compressedLen);
		copiedCompressedData.put(compressedByteArray);
		copiedCompressedData.flip();

		ByteBuffer decompressedData;
		if (isDirect) {
			decompressedData = ByteBuffer.allocateDirect(originalLen);
		} else {
			decompressedData = ByteBuffer.allocate(originalLen);
		}
		int decompressedLen = decompressor.decompress(
			copiedCompressedData, compressedOff, compressedLen, decompressedData, 0);
		assertEquals(decompressedLen, decompressedData.position());
		decompressedData.flip();

		for (int i = 0; i < decompressedLen; i++) {
			assertEquals((byte) i, decompressedData.get());
		}

		if (isDirect) {
			cleanDirectBuffer(data);
			cleanDirectBuffer(compressedData);
			cleanDirectBuffer(copiedCompressedData);
			cleanDirectBuffer(decompressedData);
		}
	}

	private void cleanDirectBuffer(ByteBuffer buffer) {
		Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
		if (cleaner != null) {
			cleaner.clean();
		}
	}
}

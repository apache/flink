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

package org.apache.flink.test.shuffle;

import org.apache.flink.api.common.io.blockcompression.AbstractBlockDecompressor;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactory;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader.CompressionMethod;
import org.apache.flink.api.common.io.blockcompression.DataCorruptionException;
import org.apache.flink.api.common.io.blockcompression.InsufficientBufferException;
import org.apache.flink.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrap {@link AbstractBlockDecompressor} to collect statistics about decompression for assertions in test cases.
 */
public class BlockDecompressorTestDelegate extends AbstractBlockDecompressor {

	private static BlockCompressionFactory internalDecompressionFactory;

	private static AtomicLong decompressionCount = new AtomicLong(0);
	private static AtomicLong totalSrcDataLength = new AtomicLong(0);
	private static AtomicLong totalDecompressedDataLength = new AtomicLong(0);

	public static void setRealDecompressorMethod(CompressionMethod decompressorMethod) {
		internalDecompressionFactory = BlockCompressionFactoryLoader.createBlockCompressionFactory(
			decompressorMethod.name(), new Configuration());
	}

	public static long getDecompressionCount() {
		return decompressionCount.get();
	}

	public static long getTotalSrcDataLength() {
		return totalSrcDataLength.get();
	}

	public static long getTotalDecompressedDataLength() {
		return totalDecompressedDataLength.get();
	}

	public static void resetStatistics() {
		decompressionCount.set(0);
		totalSrcDataLength.set(0);
		totalDecompressedDataLength.set(0);
	}

	private static void updateStatistics(int srcDataLength, int compressedDataLength) {
		decompressionCount.incrementAndGet();
		totalSrcDataLength.addAndGet(srcDataLength);
		totalDecompressedDataLength.addAndGet(compressedDataLength);
	}

	public static String getStatistics() {
		return "decompressionCount: " + decompressionCount.get() + ", totalSrcDataLength: " + totalSrcDataLength.get() +
			", totalDecompressedDataLength: " + totalDecompressedDataLength.get();
	}

	private final AbstractBlockDecompressor internalDecompressor;

	public BlockDecompressorTestDelegate() {
		this.internalDecompressor = internalDecompressionFactory.getDecompressor();
	}

	@Override
	public int decompress(ByteBuffer src, ByteBuffer dst) throws DataCorruptionException, InsufficientBufferException {
		int srcLen = src.remaining();
		int decompressedDataLength = internalDecompressor.decompress(src, dst);
		updateStatistics(srcLen, decompressedDataLength);
		return decompressedDataLength;
	}

	@Override
	public int decompress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff) throws DataCorruptionException {
		int decompressedDataLength = internalDecompressor.decompress(src, srcOff, srcLen, dst, dstOff);
		updateStatistics(srcLen, decompressedDataLength);
		return decompressedDataLength;
	}

	@Override
	public int decompress(byte[] src, byte[] dst) throws DataCorruptionException, InsufficientBufferException {
		int decompressedDataLength = internalDecompressor.decompress(src, dst);
		updateStatistics(src.length, decompressedDataLength);
		return decompressedDataLength;
	}

	@Override
	public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff) throws InsufficientBufferException, DataCorruptionException {
		int decompressedDataLength = internalDecompressor.decompress(src, srcOff, srcLen, dst, dstOff);
		updateStatistics(srcLen, decompressedDataLength);
		return decompressedDataLength;
	}
}

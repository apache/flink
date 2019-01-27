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

import org.apache.flink.api.common.io.blockcompression.AbstractBlockCompressor;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactory;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactoryLoader.CompressionMethod;
import org.apache.flink.api.common.io.blockcompression.InsufficientBufferException;
import org.apache.flink.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrap {@link AbstractBlockCompressor} to collect statistics about compression for assertions in test cases.
 */
public class BlockCompressorTestDelegate extends AbstractBlockCompressor {

	private static BlockCompressionFactory internalCompressionFactory;
	private static AtomicLong compressionCount = new AtomicLong(0);
	private static AtomicLong totalSrcDataLength = new AtomicLong(0);
	private static AtomicLong totalCompressedDataLength = new AtomicLong(0);

	public static void setRealCompressorMethod(CompressionMethod compressorMethod) {
		internalCompressionFactory = BlockCompressionFactoryLoader.createBlockCompressionFactory(
			compressorMethod.name(), new Configuration());
	}

	public static long getCompressionCount() {
		return compressionCount.get();
	}

	public static long getTotalSrcDataLength() {
		return totalSrcDataLength.get();
	}

	public static long getTotalCompressedDataLength() {
		return totalCompressedDataLength.get();
	}

	public static void resetStatistics() {
		compressionCount.set(0);
		totalSrcDataLength.set(0);
		totalCompressedDataLength.set(0);
	}

	private static void updateStatistics(int srcDataLength, int compressedDataLength) {
		compressionCount.incrementAndGet();
		totalSrcDataLength.addAndGet(srcDataLength);
		totalCompressedDataLength.addAndGet(compressedDataLength);
	}

	public static String getStatistics() {
		return "compressionCount: " + compressionCount.get() + ", totalSrcDataLength: " + totalSrcDataLength.get() +
			", totalCompressedDataLength: " + totalCompressedDataLength.get();
	}

	private final AbstractBlockCompressor internalCompressor;

	public BlockCompressorTestDelegate() {
		this.internalCompressor = internalCompressionFactory.getCompressor();
	}

	@Override
	public int getMaxCompressedSize(int srcSize) {
		return internalCompressor.getMaxCompressedSize(srcSize);
	}

	@Override
	public int getMaxCompressedSize(byte[] src) {
		return internalCompressor.getMaxCompressedSize(src);
	}

	@Override
	public int compress(ByteBuffer src, ByteBuffer dst) throws InsufficientBufferException {
		int srcLen = src.remaining();
		int compressedDataLength = internalCompressor.compress(src, dst);
		updateStatistics(srcLen, compressedDataLength);
		return compressedDataLength;
	}

	@Override
	public int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff) throws InsufficientBufferException {
		int compressedDataLength = internalCompressor.compress(src, srcOff, srcLen, dst, dstOff);
		updateStatistics(srcLen, compressedDataLength);
		return compressedDataLength;
	}

	@Override
	public int compress(byte[] src, byte[] dst) throws InsufficientBufferException {
		int compressedDataLength = internalCompressor.compress(src, dst);
		updateStatistics(src.length, compressedDataLength);
		return compressedDataLength;
	}

	@Override
	public int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff) throws InsufficientBufferException {
		int compressedDataLength = internalCompressor.compress(src, srcOff, srcLen, dst, dstOff);
		updateStatistics(srcLen, compressedDataLength);
		return compressedDataLength;
	}
}

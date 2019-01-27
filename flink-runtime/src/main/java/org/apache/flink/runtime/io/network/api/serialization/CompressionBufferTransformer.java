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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.api.common.io.blockcompression.AbstractBlockCompressor;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactory;
import org.apache.flink.api.common.io.blockcompression.InsufficientBufferException;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implementation of {@link BufferSerializationDelegate} for compression.
 */
final class CompressionBufferTransformer implements BufferSerializationDelegate {

	private static final Logger LOG = LoggerFactory.getLogger(CompressionBufferTransformer.class);

	private AbstractBlockCompressor compressor;
	private ByteBuffer bufferToBeCompressed;

	/** Used as temporary buffer for output buffer if {@link DataOutputView} for output is not backed by
	 * {@link DataOutputSerializer}. Actually {@code bufferWrapper} won't be used according to current implementation,
	 * so initialize it on demand. */
	private ResizableBuffer bufferWrapper;

	CompressionBufferTransformer(BlockCompressionFactory blockCompressionFactory) {
		this.compressor = blockCompressionFactory.getCompressor();
	}

	@Override
	public void setBuffer(ByteBuffer buffer) {
		bufferToBeCompressed = buffer;
	}

	@Override
	public void clear() {
		bufferToBeCompressed = null;
		if (bufferWrapper != null) {
			bufferWrapper.clear();
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		ByteBuffer inputByteBuffer = bufferToBeCompressed;
		final int prevInputPosition = inputByteBuffer.position();
		final int lengthBeforeCompression = inputByteBuffer.remaining();
		int lengthAfterCompression;

		// Reserve 8 bytes to hold two integer: length before compression and length after compression.
		int maxCompressedLen = compressor.getMaxCompressedSize(lengthBeforeCompression) + 8;

		ByteBuffer outputByteBuffer;
		if (out instanceof DataOutputSerializer) {
			DataOutputSerializer dataOutputSerializer = ((DataOutputSerializer) out);
			int prevOutputPosition = dataOutputSerializer.position();
			try {
				dataOutputSerializer.ensureCapacity(prevOutputPosition + maxCompressedLen);
				outputByteBuffer = ((DataOutputSerializer) out).wrapAsWritableByteBuffer();
				outputByteBuffer.position(prevOutputPosition + 8);
				lengthAfterCompression = compressor.compress(inputByteBuffer, outputByteBuffer);
			} catch (InsufficientBufferException e) {
				LOG.warn("Compressed buffer is larger than the original size, original buffer size " +
					lengthBeforeCompression);
				// This case rarely occurs which mean such compression algorithm is
				// inappropriate in such scenario. But we still try to fulfill the
				// the compression by enlarging the output buffer to one and a half of
				// the original buffer size.
				inputByteBuffer.position(prevInputPosition);
				dataOutputSerializer.ensureCapacity(prevOutputPosition + maxCompressedLen + (maxCompressedLen >> 1));
				outputByteBuffer = ((DataOutputSerializer) out).wrapAsWritableByteBuffer();
				outputByteBuffer.position(prevOutputPosition + 8);
				lengthAfterCompression = compressor.compress(inputByteBuffer, outputByteBuffer);
			}
			assert outputByteBuffer.position() == prevOutputPosition + lengthAfterCompression + 8;
			outputByteBuffer.position(prevOutputPosition);
			outputByteBuffer.putInt(lengthAfterCompression);
			outputByteBuffer.putInt(lengthBeforeCompression);
			dataOutputSerializer.position(prevOutputPosition + lengthAfterCompression + 8);
		} else {
			try {
				if (bufferWrapper == null) {
					bufferWrapper = new ResizableBuffer();
				}
				bufferWrapper.resetCapacity(maxCompressedLen);
				outputByteBuffer = ByteBuffer.wrap(bufferWrapper.getHeapMemory());
				outputByteBuffer.position(8);
				lengthAfterCompression = compressor.compress(inputByteBuffer, outputByteBuffer);
			} catch (InsufficientBufferException e) {
				LOG.warn("Compressed buffer is larger than the original size, original buffer size " +
					lengthBeforeCompression);
				// This case rarely occurs which mean such compression algorithm is
				// inappropriate in such scenario. But we still try to fulfill the
				// the compression by enlarging the output buffer to one and a half of
				// the original buffer size.
				inputByteBuffer.position(prevInputPosition);
				bufferWrapper.resetCapacity(maxCompressedLen + (maxCompressedLen >> 1));
				outputByteBuffer = ByteBuffer.wrap(bufferWrapper.getHeapMemory());
				outputByteBuffer.position(8);
				lengthAfterCompression = compressor.compress(inputByteBuffer, outputByteBuffer);
			}
			assert outputByteBuffer.position() == lengthAfterCompression + 8;
			outputByteBuffer.position(0);
			outputByteBuffer.putInt(lengthAfterCompression);
			outputByteBuffer.putInt(lengthBeforeCompression);
			Buffer currBuffer = bufferWrapper.getBuffer();
			currBuffer.setReaderIndex(0);
			currBuffer.setSize(lengthAfterCompression + 8);
			out.write(currBuffer.getMemorySegment(), currBuffer.getReaderIndex(), currBuffer.getSize());

			bufferWrapper.recycle();
		}

		bufferToBeCompressed = null;
	}

}

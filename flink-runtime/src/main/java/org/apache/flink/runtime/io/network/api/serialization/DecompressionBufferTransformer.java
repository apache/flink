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

import org.apache.flink.api.common.io.blockcompression.AbstractBlockDecompressor;
import org.apache.flink.api.common.io.blockcompression.BlockCompressionFactory;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implementation of {@link BufferDeserializationDelegate} for decompression.
 */
final class DecompressionBufferTransformer implements BufferDeserializationDelegate {

	private final AbstractBlockDecompressor decompressor;
	private final ResizableBuffer bufferWrapper;

	DecompressionBufferTransformer(BlockCompressionFactory blockCompressionFactory) {
		this.decompressor = blockCompressionFactory.getDecompressor();
		this.bufferWrapper = new ResizableBuffer();
	}

	@Override
	public void reset() {
		bufferWrapper.recycle();
	}

	@Override
	public Buffer getBuffer() {
		return bufferWrapper.getBuffer();
	}

	@Override
	public void clear() {
		bufferWrapper.clear();
	}

	@Override
	public void read(DataInputView in) throws IOException {
		int lengthBeforeDecompression = in.readInt();
		int lengthAfterDecompression = in.readInt();

		bufferWrapper.resetCapacity(lengthAfterDecompression);
		ByteBuffer outputByteBuffer = ByteBuffer.wrap(bufferWrapper.getHeapMemory());

		ByteBuffer inputByteBuffer;
		if (in instanceof NonSpanningWrapper) {
			inputByteBuffer = ((NonSpanningWrapper) in).wrapAsByteBuffer(lengthBeforeDecompression);
		} else if (in instanceof DataInputDeserializer) {
			inputByteBuffer = ((DataInputDeserializer) in).wrapAsByteBuffer(lengthBeforeDecompression);
		} else {
			byte[] bytes = new byte[lengthBeforeDecompression];
			in.readFully(bytes, 0, lengthBeforeDecompression);
			inputByteBuffer = ByteBuffer.wrap(bytes);
		}

		decompressor.decompress(inputByteBuffer, outputByteBuffer);

		assert outputByteBuffer.position() == lengthAfterDecompression;
		Buffer currentBuffer = bufferWrapper.getBuffer();
		currentBuffer.setReaderIndex(0);
		currentBuffer.setSize(lengthAfterDecompression);

		if (in instanceof  NonSpanningWrapper) {
			in.skipBytesToRead(lengthBeforeDecompression);
		} else if (in instanceof DataInputDeserializer) {
			in.skipBytesToRead(lengthBeforeDecompression);
		}
	}
}

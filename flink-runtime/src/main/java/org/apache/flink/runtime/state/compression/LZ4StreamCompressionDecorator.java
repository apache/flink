/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.compression;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.util.NonClosingInputStreamDecorator;
import org.apache.flink.runtime.util.NonClosingOutpusStreamDecorator;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This implementation decorates the stream with LZ4 compression.
 */
@Internal
public class LZ4StreamCompressionDecorator extends StreamCompressionDecorator {

	public static final LZ4StreamCompressionDecorator INSTANCE = new LZ4StreamCompressionDecorator();

	private static final long serialVersionUID = 1L;

	private static final int BLOCK_SIZE_256K = 256 * 1024;

	@Override
	protected OutputStream decorateWithCompression(NonClosingOutpusStreamDecorator stream) throws IOException {
		return new LZ4BlockOutputStream(stream, BLOCK_SIZE_256K);
	}

	@Override
	protected InputStream decorateWithCompression(NonClosingInputStreamDecorator stream) throws IOException {
		boolean stopOnEmptyBlock = false;
		return new LZ4BlockInputStream(stream, stopOnEmptyBlock);
	}

	@Override
	public StreamCompressionDecoratorSnapshot snapshotConfiguration() {
		return LZ4CompressionDecoratorSnapshot.INSTANCE;
	}

	public static class LZ4CompressionDecoratorSnapshot extends SimpleStreamCompressionDecoratorSnapshot {

		public static final LZ4CompressionDecoratorSnapshot INSTANCE = new LZ4CompressionDecoratorSnapshot();

		public LZ4CompressionDecoratorSnapshot() {
			super(LZ4StreamCompressionDecorator.class);
		}
	}
}

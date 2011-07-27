/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.compression.library.dynamic;

import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.compression.AbstractCompressionLibrary;
import eu.stratosphere.nephele.io.compression.Compressor;
import eu.stratosphere.nephele.io.compression.Decompressor;

public class StubCompressionLibrary extends AbstractCompressionLibrary {

	@Override
	public int getUncompressedBufferSize(int compressedBufferSize) {

		// No overhead, since there is actually no compression
		return compressedBufferSize;
	}

	@Override
	public String getLibraryName() {
		
		return "STUB";
	}

	@Override
	protected Compressor initNewCompressor(final AbstractByteBufferedOutputChannel<?> outputChannel) {

		return new StubCompressor(this);
	}

	@Override
	protected Decompressor initNewDecompressor(final AbstractByteBufferedInputChannel<?> inputChannel) {

		return new StubDecompressor(this);
	}

}

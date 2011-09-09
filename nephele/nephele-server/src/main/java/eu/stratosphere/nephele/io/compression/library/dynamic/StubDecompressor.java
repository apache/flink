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

import eu.stratosphere.nephele.io.compression.AbstractDecompressor;

public class StubDecompressor extends AbstractDecompressor {

	@Override
	protected int decompressBytesDirect(int offset) {

		// System.out.println("Compressed buffer length: " + this.compressedDataBuffer);

		this.uncompressedDataBufferLength = this.compressedDataBuffer.limit();
		this.uncompressedDataBuffer.position(0);
		this.uncompressedDataBuffer.limit(this.uncompressedDataBufferLength);

		this.uncompressedDataBuffer.put(this.compressedDataBuffer);

		// System.out.println("Uncompressed buffer position: " + this.uncompressedDataBuffer.position());

		return this.uncompressedDataBuffer.position() - SIZE_LENGTH; // TODO: Improve code style here
	}

}

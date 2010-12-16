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

package eu.stratosphere.nephele.io.compression;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;

public interface Decompressor {

	/**
	 * Method to decompress the data from the compressed Data-Buffer to the uncompressed Data-Buffer.
	 */
	void decompress() throws IOException;

	/**
	 * Get the uncompressed Data-Buffer.
	 * 
	 * @return the Data-Buffer used to store the uncompressed/output data.
	 */
	Buffer getUncompresssedDataBuffer();

	/**
	 * Get the compressed Data-Buffer.
	 * 
	 * @return the Data-Buffer used to store the compressed/input data.
	 */
	Buffer getCompressedDataBuffer();

	/**
	 * Set the compressed Data-Buffer. This method is used together with NetworkInputChannels, where we
	 * use multiply Buffers for the compressed Data.
	 * 
	 * @param buffer
	 *        the Data-Buffer used to store the compressed/input data
	 */
	void setCompressedDataBuffer(Buffer buffer);

	/**
	 * Set the uncompressed data buffer.
	 * 
	 * @param buffer
	 *        the uncompressed data buffer
	 */
	void setUncompressedDataBuffer(Buffer buffer);

	/**
	 * Sets the internal decompression library index, as reported by the
	 * compressor.
	 * This method must not be called when any compression level than
	 * dynamic compression is activated.
	 * 
	 * @param index
	 *        the current internal decompression library index
	 */
	void setCurrentInternalDecompressionLibraryIndex(int index);
}

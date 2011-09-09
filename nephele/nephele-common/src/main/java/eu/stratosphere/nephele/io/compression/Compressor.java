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

public interface Compressor {

	/**
	 * Method to compress the data from the uncompressed Data-Buffer to the compressed Data-Buffer.
	 */
	void compress() throws IOException;

	/**
	 * Get the uncompressed Data-Buffer.
	 * 
	 * @return the Data-Buffer used to store the uncompressed/input data.
	 */
	Buffer getUncompresssedDataBuffer();

	/**
	 * Get the compressed Data-Buffer.
	 * 
	 * @return the Data-Buffer used to store the compressed/output data.
	 */
	Buffer getCompressedDataBuffer();

	/**
	 * Sets the uncompressed data buffer.
	 * 
	 * @param buffer
	 *        the buffer which contains the uncompressed data
	 */
	void setUncompressedDataBuffer(Buffer buffer);

	/**
	 * Set the compressed Data-Buffer. This method is used together with NetworkOutputChannels, where we
	 * use multiply Buffers for the compressed Data.
	 * 
	 * @param buffer
	 *        the Data-Buffer used to store the compressed/output data
	 */
	void setCompressedDataBuffer(Buffer buffer);

	/**
	 * Returns the current internal compression library index. The index points to the
	 * concrete compression library that is used if dynamic compression is enabled.
	 * For all other compression libraries the index is always <code>0</code>.
	 * 
	 * @return the current internal compression library index in case of dynamic compression
	 *         or <code>0</code> for any other compression level.
	 */
	int getCurrentInternalCompressionLibraryIndex();
	
	/**
	 * Stops the compressor and releases all allocated internal resources.
	 */
	void shutdown();
}

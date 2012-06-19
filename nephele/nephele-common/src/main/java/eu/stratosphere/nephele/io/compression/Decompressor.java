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
	 * Decompresses the data in the given buffer.
	 * 
	 * @param compressedData
	 *        the buffer containing the compressed data
	 * @return a buffer containing the uncompressed data
	 * @throws IOException
	 *         thrown if an error occurs during the decompression process
	 */
	Buffer decompress(Buffer compressedData) throws IOException;

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

	/**
	 * Stops the decompressor and releases all allocated internal resources.
	 */
	void shutdown();
}

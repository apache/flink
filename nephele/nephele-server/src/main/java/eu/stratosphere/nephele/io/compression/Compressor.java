/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
	 * Compresses the data included in the given buffer.
	 * 
	 * @param uncompressedData
	 *        the buffer containing the uncompressed data
	 * @return the buffer containing the compressed buffers
	 * @throws IOException
	 *         thrown if an error occurs during the compression
	 * @throws InterruptedException
	 *         thrown if the compressor is interrupted while waiting for a buffer to compress the data into
	 */
	Buffer compress(Buffer uncompressedData) throws IOException;

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
	 * Notifies the compressor that is it now by another output channel.
	 */
	void increaseChannelCounter();

	/**
	 * Stops the compressor and releases all allocated internal resources if the compressor object is no longer used by
	 * any output channel.
	 */
	void shutdown();
}

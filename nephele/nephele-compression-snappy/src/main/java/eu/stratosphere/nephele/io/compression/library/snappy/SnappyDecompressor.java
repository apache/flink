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

package eu.stratosphere.nephele.io.compression.library.snappy;

import eu.stratosphere.nephele.io.compression.AbstractDecompressor;
import eu.stratosphere.nephele.io.compression.CompressionBufferProvider;

/**
 * This class provides an interface for decompressing byte-buffers with the native snappy library.
 * <p>
 * http://code.google.com/p/snappy/
 * 
 * @author warneke
 */
public class SnappyDecompressor extends AbstractDecompressor {

	SnappyDecompressor(final CompressionBufferProvider bufferProvider) {
		super(bufferProvider);
	}

	native static void initIDs();

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected native int decompressBytesDirect(int offset);
}

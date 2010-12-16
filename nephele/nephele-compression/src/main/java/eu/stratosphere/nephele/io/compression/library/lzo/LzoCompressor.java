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

package eu.stratosphere.nephele.io.compression.library.lzo;

import eu.stratosphere.nephele.io.compression.AbstractCompressor;
import eu.stratosphere.nephele.io.compression.CompressionException;

/**
 * This class provides an interface for compressing byte-buffers with the native miniLZO library.
 * 
 * @author akli
 */

public class LzoCompressor extends AbstractCompressor {

	public LzoCompressor()
							throws CompressionException {

		// TODO: Check where to put this comment
		/*
		 * Calculate size of compressed data buffer according to
		 * LZO Manual http://www.oberhumer.com/opensource/lzo/lzofaq.php
		 */

		init();
	}

	native static void initIDs();

	private native void init();

	protected native int compressBytesDirect(int offset);
}

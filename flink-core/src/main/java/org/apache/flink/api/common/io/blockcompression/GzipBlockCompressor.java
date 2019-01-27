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

package org.apache.flink.api.common.io.blockcompression;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.IOException;

/**
 * Compress data into Gzip format.
 *
 * Note that this class is only a wrapper of {@link GzipCompressorOutputStream}.
 * For efficiency, one should rewrite this class.
 */
public class GzipBlockCompressor extends AbstractBlockCompressor {
	private NoCopyByteArrayOutputStream dstStream;

	@Override
	public int getMaxCompressedSize(int srcSize) {
		// FIXME return a reasonable estimation instead
		return srcSize + 64;
	}

	@Override
	public int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff) {
		try {
			if (dstStream == null) {
				dstStream = new NoCopyByteArrayOutputStream(dst, dstOff);
			} else {
				dstStream.reuse(dst, dstOff);
			}
			GzipCompressorOutputStream compressStream = new GzipCompressorOutputStream(dstStream);

			compressStream.write(src, srcOff, srcLen);
			compressStream.close();

			int compressedLen = dstStream.getNumBytesWritten();
			dstStream.close();

			if (compressedLen > (dst.length - dstOff)) {
				throw new InsufficientBufferException("destination buffer remains " + (dst.length - dstOff) +
					" bytes, requires " + compressedLen + " bytes.");
			}

			return compressedLen;
		} catch (IOException e) {
			throw new DataCorruptionException(e);
		}
	}
}

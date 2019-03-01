/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util;

import org.apache.flink.table.dataformat.BinaryString;

/**
 * Util for {@link BinaryString}.
 */
public class BinaryStringUtil {

	/**
	 * SQL execution threads is limited, not too many, so it can bear the overhead of 64K per thread.
	 */
	private static final int MAX_BYTES_LENGTH = 1024 * 64;
	private static final ThreadLocal<byte[]> BYTES_LOCAL = new ThreadLocal<>();

	/**
	 * Allocate bytes that is only for temporary usage, it should not be stored in somewhere else.
	 * Use a {@link ThreadLocal} to reuse bytes to avoid overhead of byte[] new and gc.
	 *
	 * <p>If there are methods that can only accept a byte[], instead of a MemorySegment[]
	 * parameter, we can allocate a reuse bytes and copy the MemorySegment data to byte[],
	 * then call the method. Such as String deserialization.
	 */
	public static byte[] allocateReuseBytes(int length) {
		byte[] bytes = BYTES_LOCAL.get();

		if (bytes == null) {
			if (length <= MAX_BYTES_LENGTH) {
				bytes = new byte[MAX_BYTES_LENGTH];
				BYTES_LOCAL.set(bytes);
			} else {
				bytes = new byte[length];
			}
		} else if (bytes.length < length) {
			bytes = new byte[length];
		}

		return bytes;
	}
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.vector;

/**
 * Bytes column vector to get {@link Bytes}, it include original data and offset and length.
 * The data in {@link Bytes} maybe reuse.
 */
public interface BytesColumnVector extends ColumnVector {
	Bytes getBytes(int i);

	/**
	 * Bytes data.
	 */
	class Bytes {
		public final byte[] data;
		public final int offset;
		public final int len;

		public Bytes(byte[] data, int offset, int len) {
			this.data = data;
			this.offset = offset;
			this.len = len;
		}

		public byte[] getBytes() {
			if (offset == 0 && len == data.length) {
				return data;
			}
			byte[] res = new byte[len];
			System.arraycopy(data, offset, res, 0, len);
			return res;
		}
	}
}

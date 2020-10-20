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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A wrapper of the byte array. This class is used to calculate a deterministic hash code of a byte array.
 */
@Internal
public class ByteArrayWrapper implements Serializable {

	private static final long serialVersionUID = 1L;

	private byte[] data;

	private int offset;

	private int limit;

	public ByteArrayWrapper(byte[] data) {
		this.data = data;
		this.offset = 0;
		this.limit = data.length;
	}

	public ByteArrayWrapper(byte[] data, int offset) {
		this.data = data;
		this.offset = offset;
		this.limit = data.length;
	}

	public ByteArrayWrapper(byte[] data, int offset, int limit) {
		this.data = data;
		this.offset = offset;
		this.limit = limit;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public byte get(int i) {
		return data[i + offset];
	}

	@Override
	public int hashCode() {
		int hash = 1;
		for (int i = limit - 1; i >= offset; i--) {
			hash = hash * 31 + data[i];
		}
		return hash;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof ByteArrayWrapper) {
			ByteArrayWrapper otherByteArrayWrapper = (ByteArrayWrapper) other;
			if (limit - offset != otherByteArrayWrapper.limit - otherByteArrayWrapper.offset) {
				return false;
			} else {
				for (int i = limit - offset - 1; i >= 0; i--) {
					if (get(i) != otherByteArrayWrapper.get(i)) {
						return false;
					}
				}
				return true;
			}
		} else {
			return false;
		}
	}

	public ByteArrayWrapper copy() {
		return new ByteArrayWrapper(Arrays.copyOfRange(data, offset, limit));
	}
}

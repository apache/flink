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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.AbstractCloseableHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * A state handle that contains stream state in a byte array.
 */
public class ByteStreamStateHandle extends AbstractCloseableHandle implements StreamStateHandle {

	private static final long serialVersionUID = -5280226231200217594L;

	/**
	 * the state data
	 */
	protected final byte[] data;

	/**
	 * Creates a new ByteStreamStateHandle containing the given data.
	 *
	 * @param data The state data.
	 */
	public ByteStreamStateHandle(byte[] data) {
		this.data = data;
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		ensureNotClosed();

		FSDataInputStream inputStream = new FSDataInputStream() {
			int index = 0;

			@Override
			public void seek(long desired) throws IOException {
				Preconditions.checkArgument(desired >= 0 && desired < Integer.MAX_VALUE);
				index = (int) desired;
			}

			@Override
			public long getPos() throws IOException {
				return index;
			}

			@Override
			public int read() throws IOException {
				return index < data.length ? data[index++] & 0xFF : -1;
			}
		};
		registerCloseable(inputStream);
		return inputStream;
	}

	public byte[] getData() {
		return data;
	}

	@Override
	public void discardState() {
	}

	@Override
	public long getStateSize() {
		return data.length;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ByteStreamStateHandle)) {
			return false;
		}

		ByteStreamStateHandle that = (ByteStreamStateHandle) o;
		return Arrays.equals(data, that.data);

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + Arrays.hashCode(data);
		return result;
	}

	public static StreamStateHandle fromSerializable(Serializable value) throws IOException {
		return new ByteStreamStateHandle(InstantiationUtil.serializeObject(value));
	}
}

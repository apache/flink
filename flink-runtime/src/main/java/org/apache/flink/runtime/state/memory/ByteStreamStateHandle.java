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
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A state handle that contains stream state in a byte array.
 */
public class ByteStreamStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = -5280226231202517594L;

	/**
	 * The state data.
	 */
	protected final byte[] data;

	/**
	 * A unique name of by which this state handle is identified and compared. Like a filename, all
	 * {@link ByteStreamStateHandle} with the exact same name must also have the exact same content in data.
	 */
	protected final String handleName;

	/**
	 * Creates a new ByteStreamStateHandle containing the given data.
	 */
	public ByteStreamStateHandle(String handleName, byte[] data) {
		this.handleName = Preconditions.checkNotNull(handleName);
		this.data = Preconditions.checkNotNull(data);
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return new ByteStateHandleInputStream();
	}

	public byte[] getData() {
		return data;
	}

	public String getHandleName() {
		return handleName;
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
		return handleName.equals(that.handleName);
	}

	@Override
	public int hashCode() {
		return 31 * handleName.hashCode();
	}

	@Override
	public String toString() {
		return "ByteStreamStateHandle{" +
			"handleName='" + handleName + '\'' +
			", dataBytes=" + data.length +
			'}';
	}

	/**
	 * An input stream view on a byte array.
	 */
	private final class ByteStateHandleInputStream extends FSDataInputStream {

		private int index;

		public ByteStateHandleInputStream() {
			this.index = 0;
		}

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
	}
}

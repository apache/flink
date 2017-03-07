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

package org.apache.flink.migration.runtime.state.memory;

import org.apache.flink.migration.runtime.state.AbstractCloseableHandle;
import org.apache.flink.migration.runtime.state.StateHandle;
import org.apache.flink.migration.runtime.state.StreamStateHandle;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

@Deprecated
@SuppressWarnings("deprecation")
public final class ByteStreamStateHandle extends AbstractCloseableHandle implements StreamStateHandle {

	private static final long serialVersionUID = -5280226231200217594L;
	
	/** the state data */
	private final byte[] data;

	/**
	 * Creates a new ByteStreamStateHandle containing the given data.
	 * 
	 * @param data The state data.
	 */
	public ByteStreamStateHandle(byte[] data) {
		this.data = data;
	}

	@Override
	public InputStream getState(ClassLoader userCodeClassLoader) throws Exception {
		ensureNotClosed();

		ByteArrayInputStream stream = new ByteArrayInputStream(data);
		registerCloseable(stream);

		return stream;
	}

	@Override
	public void discardState() {}

	@Override
	public long getStateSize() {
		return data.length;
	}

	@Override
	public <T extends Serializable> StateHandle<T> toSerializableHandle() {
		SerializedStateHandle<T> serializableHandle = new SerializedStateHandle<T>(data);

		// forward the closed status
		if (isClosed()) {
			try {
				serializableHandle.close();
			} catch (IOException e) {
				// should not happen on a fresh handle, but forward anyways
				throw new RuntimeException(e);
			}
		}

		return serializableHandle;
	}

	public byte[] getData() {
		return data;
	}
}

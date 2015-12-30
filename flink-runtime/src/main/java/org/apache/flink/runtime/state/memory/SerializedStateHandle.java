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

import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * A state handle that represents its state in serialized form as bytes.
 *
 * @param <T> The type of state represented by this state handle.
 */
public class SerializedStateHandle<T extends Serializable> implements StateHandle<T> {
	
	private static final long serialVersionUID = 4145685722538475769L;

	/** The serialized data */
	private final byte[] serializedData;
	
	/**
	 * Creates a new serialized state handle, eagerly serializing the given state object.
	 * 
	 * @param value The state object.
	 * @throws IOException Thrown, if the serialization fails.
	 */
	public SerializedStateHandle(T value) throws IOException {
		this.serializedData = value == null ? null : InstantiationUtil.serializeObject(value);
	}

	/**
	 * Creates a new serialized state handle, based in the given already serialized data.
	 * 
	 * @param serializedData The serialized data.
	 */
	public SerializedStateHandle(byte[] serializedData) {
		this.serializedData = serializedData;
	}
	
	@Override
	public T getState(ClassLoader classLoader) throws Exception {
		if (classLoader == null) {
			throw new NullPointerException();
		}

		return serializedData == null ? null : InstantiationUtil.<T>deserializeObject(serializedData, classLoader);
	}

	/**
	 * Gets the size of the serialized state.
	 * @return The size of the serialized state.
	 */
	public int getSizeOfSerializedState() {
		return serializedData.length;
	}

	/**
	 * Discarding heap-memory backed state is a no-op, so this method does nothing.
	 */
	@Override
	public void discardState() {}
}

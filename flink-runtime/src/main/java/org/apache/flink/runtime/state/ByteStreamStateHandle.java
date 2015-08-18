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

package org.apache.flink.runtime.state;

import org.apache.flink.util.InstantiationUtil;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Statehandle that writes/reads the contents of the serializable checkpointed
 * state to the provided input and outputstreams using default java
 * serialization.
 * 
 */
public abstract class ByteStreamStateHandle implements StateHandle<Serializable> {

	private static final long serialVersionUID = -962025800339325828L;

	private transient Serializable state;
	private boolean isWritten = false;

	public ByteStreamStateHandle(Serializable state) {
		if (state != null) {
			this.state = state;
		} else {
			throw new RuntimeException("State cannot be null");
		}
	}

	/**
	 * The state will be written to the stream returned by this method.
	 */
	protected abstract OutputStream getOutputStream() throws Exception;

	/**
	 * The state will be read from the stream returned by this method.
	 */
	protected abstract InputStream getInputStream() throws Exception;

	@Override
	public Serializable getState(ClassLoader userCodeClassLoader) throws Exception {
		if (!stateFetched()) {
			ObjectInputStream stream = new InstantiationUtil.ClassLoaderObjectInputStream(getInputStream(), userCodeClassLoader);
			try {
				state = (Serializable) stream.readObject();
			} finally {
				stream.close();
			}
		}
		return state;
	}

	private void writeObject(ObjectOutputStream oos) throws Exception {
		if (!isWritten) {
			ObjectOutputStream stream = new ObjectOutputStream(getOutputStream());
			try {
				stream.writeObject(state);
				isWritten = true;
			} finally {
				stream.close();
			}
		}
		oos.defaultWriteObject();
	}

	/**
	 * Checks whether the state has already been fetched from the remote
	 * storage.
	 */
	public boolean stateFetched() {
		return state != null;
	}
	
	/**
	 * Checks whether the state has already been written to the external store
	 */
	public boolean isWritten() {
		return isWritten;
	}
}

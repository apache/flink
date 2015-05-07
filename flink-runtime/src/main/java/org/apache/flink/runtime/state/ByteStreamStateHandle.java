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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

public abstract class ByteStreamStateHandle implements StateHandle<Serializable> {

	private static final long serialVersionUID = -962025800339325828L;

	transient Serializable state;

	public ByteStreamStateHandle(Serializable state) throws IOException {
		this.state = state;
	}

	protected abstract OutputStream getOutputStream() throws Exception;

	protected abstract InputStream getInputStream() throws Exception;

	@Override
	public Serializable getState() throws Exception {
		ObjectInputStream stream = new ObjectInputStream(getInputStream());
		state = (Serializable) stream.readObject();
		stream.close();
		return state;
	}

	private void writeObject(ObjectOutputStream oos) throws Exception {
		ObjectOutputStream stream = new ObjectOutputStream(getOutputStream());
		stream.writeObject(state);
		stream.close();
		oos.defaultWriteObject();
	}
}

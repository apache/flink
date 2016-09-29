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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class JavaSerializer<T extends Serializable> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<T> duplicate() {
		return this;
	}

	@Override
	public T createInstance() {
		return null;
	}

	@Override
	public T copy(T from) {

		try {
			return InstantiationUtil.clone(from);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("Could not copy instance of " + from + '.', e);
		}
	}

	@Override
	public T copy(T from, T reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new DataOutputViewStream(target));
		oos.writeObject(record);
		oos.flush();
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		ObjectInputStream ois = new ObjectInputStream(new DataInputViewStream(source));

		try {
			@SuppressWarnings("unchecked")
			T nfa = (T) ois.readObject();
			return nfa;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not deserialize NFA.", e);
		}
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int size = source.readInt();
		target.writeInt(size);
		target.write(source, size);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof JavaSerializer && ((JavaSerializer<T>) obj).canEqual(this);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof JavaSerializer;
	}

	@Override
	public int hashCode() {
		return getClass().hashCode();
	}
}

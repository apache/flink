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

package org.apache.flink.runtime.util;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.StringUtils;

/**
 * This class extends a standard {@link java.util.ArrayList} by implementing the
 * {@link org.apache.flink.core.io.IOReadableWritable} interface.
 *
 * @param <E>
 *        the type of object stored inside this array list
 */
public class SerializableArrayList<E extends IOReadableWritable> extends ArrayList<E> implements IOReadableWritable {

	private static final long serialVersionUID = 8196856588290198537L;

	/**
	 * Constructs an empty list with an initial capacity of ten.
	 */
	public SerializableArrayList() {
		super();
	}

	/**
	 * Constructs an empty list with the specified initial capacity.
	 * 
	 * @param initialCapacity
	 *        the initial capacity of the list
	 */
	public SerializableArrayList(final int initialCapacity) {
		super(initialCapacity);
	}

	@Override
	public void write(final DataOutputView out) throws IOException {

		out.writeInt(size());
		for (E element : this) {
			// Write out type
			StringUtils.writeNullableString(element.getClass().getName(), out);
			// Write out element itself
			element.write(out);
		}
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		// Make sure the list is empty
		clear();

		final int numberOfElements = in.readInt();
		for (int i = 0; i < numberOfElements; i++) {
			final String elementType = StringUtils.readNullableString(in);

			E element;
			try {
				@SuppressWarnings("unchecked")
				Class<E> clazz = (Class<E>) Class.forName(elementType);
				element = clazz.newInstance();
			}
			catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			catch (Exception e) {
				throw new IOException(e);
			}

			element.read(in);
			add(element);
		}
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof SerializableArrayList<?> && super.equals(obj);
	}
}

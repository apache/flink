/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.util.StringUtils;

/**
 * This class extends a standard {@link java.util.ArrayList} by implementing the
 * {@link eu.stratosphere.core.io.IOReadableWritable} interface. As a result, array lists of this type can be used
 * with Nephele's RPC system.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 * @param <E>
 *        the type of object stored inside this array list
 */
public class SerializableArrayList<E extends IOReadableWritable> extends ArrayList<E> implements IOReadableWritable {

	/**
	 * Generated serial version UID.
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(size());
		final Iterator<E> it = iterator();
		while (it.hasNext()) {

			final E element = it.next();
			// Write out type
			StringRecord.writeString(out, element.getClass().getName());
			// Write out element itself
			element.write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInput in) throws IOException {

		// Make sure the list is empty
		clear();
		final int numberOfElements = in.readInt();
		for (int i = 0; i < numberOfElements; i++) {
			final String elementType = StringRecord.readString(in);
			Class<E> clazz = null;
			try {
				clazz = (Class<E>) Class.forName(elementType);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			E element = null;
			try {
				element = clazz.newInstance();
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			element.read(in);
			add(element);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof SerializableArrayList<?>)) {
			return false;
		}

		final SerializableArrayList<?> sal = (SerializableArrayList<?>) obj;

		if (this.size() != sal.size()) {
			return false;
		}

		final Iterator<E> it = iterator();
		final Iterator<?> it2 = sal.iterator();
		while (it.hasNext()) {

			final E e = it.next();
			final Object obj2 = it2.next();
			if (!e.equals(obj2)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		int hashCode = Integer.MIN_VALUE;

		if (!isEmpty()) {
			final E e = get(0);
			hashCode += Math.abs(e.getClass().hashCode());
		}

		hashCode += size();

		return hashCode;
	}
}

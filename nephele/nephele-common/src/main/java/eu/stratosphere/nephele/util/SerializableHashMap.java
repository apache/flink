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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * This class extends a standard {@link java.util.HashMap} by implementing the
 * {@link eu.stratosphere.nephele.io.IOReadableWritable} interface. As a result, hash maps of this type can be used
 * with Nephele's RPC system.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 * @param <K>
 *        the type of the key used in this hash map
 * @param <V>
 *        the type of the value used in this hash map
 */
public class SerializableHashMap<K extends IOReadableWritable, V extends IOReadableWritable> extends HashMap<K, V>
		implements IOReadableWritable {

	/**
	 * The generated serial version UID.
	 */
	private static final long serialVersionUID = 6693468726881121924L;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(size());

		final Iterator<Map.Entry<K, V>> it = entrySet().iterator();

		while (it.hasNext()) {

			final Map.Entry<K, V> entry = it.next();
			final K key = entry.getKey();
			final V value = entry.getValue();
			StringRecord.writeString(out, key.getClass().getName());
			key.write(out);
			StringRecord.writeString(out, value.getClass().getName());
			value.write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInput in) throws IOException {

		final int numberOfMapEntries = in.readInt();

		for (int i = 0; i < numberOfMapEntries; i++) {

			final String keyType = StringRecord.readString(in);
			Class<K> keyClass = null;
			try {
				keyClass = (Class<K>) Class.forName(keyType);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			K key = null;
			try {
				key = keyClass.newInstance();
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			key.read(in);

			final String valueType = StringRecord.readString(in);
			Class<V> valueClass = null;
			try {
				valueClass = (Class<V>) Class.forName(valueType);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			V value = null;
			try {
				value = valueClass.newInstance();
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}
			value.read(in);

			put(key, value);
		}

	}
}

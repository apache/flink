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
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.StringUtils;

/**
 * This class extends a standard {@link java.util.HashMap} by implementing the
 * {@link org.apache.flink.core.io.IOReadableWritable} interface.
 * 
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


	@Override
	public void write(final DataOutputView out) throws IOException {
		
		out.writeInt(size());

		for (Map.Entry<K, V> entry : entrySet()) {
			final K key = entry.getKey();
			final V value = entry.getValue();

			StringUtils.writeNullableString(key.getClass().getName(), out);
			key.write(out);

			StringUtils.writeNullableString(value.getClass().getName(), out);
			value.write(out);
		}
	}


	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInputView in) throws IOException {
		
		final int numberOfMapEntries = in.readInt();

		for (int i = 0; i < numberOfMapEntries; i++) {

			final String keyType = StringUtils.readNullableString(in);
			Class<K> keyClass;
			try {
				keyClass = (Class<K>) Class.forName(keyType);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}

			K key = null;
			try {
				key = keyClass.newInstance();
			} catch (Exception e) {
				throw new IOException(e);
			}

			key.read(in);

			final String valueType = StringUtils.readNullableString(in);
			Class<V> valueClass;
			try {
				valueClass = (Class<V>) Class.forName(valueType);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}

			V value;
			try {
				value = valueClass.newInstance();
			} catch (Exception e) {
				throw new IOException(e);
			}
			value.read(in);

			put(key, value);
		}

	}
}

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
import java.util.HashSet;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.StringUtils;

/**
 * This class extends a standard {@link java.util.HashSet} by implementing the
 * {@link org.apache.flink.core.io.IOReadableWritable} interface.
 * 
 * @param <T>
 *        the type used in this hash set
 */
public class SerializableHashSet<T extends IOReadableWritable> extends HashSet<T> implements IOReadableWritable {

	/**
	 * The generated serial version UID.
	 */
	private static final long serialVersionUID = -4615823301768215807L;


	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeInt(size());

		for (T entry : this) {
			StringUtils.writeNullableString(entry.getClass().getName(), out);
			entry.write(out);
		}
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		final int numberOfMapEntries = in.readInt();

		for (int i = 0; i < numberOfMapEntries; i++) {
			final String type = StringUtils.readNullableString(in);

			T entry;
			try {
				@SuppressWarnings("unchecked")
				Class<T> clazz = (Class<T>) Class.forName(type);
				entry = clazz.newInstance();
			}
			catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			catch (Exception e) {
				throw new IOException(e);
			}

			entry.read(in);
			add(entry);
		}
	}
}

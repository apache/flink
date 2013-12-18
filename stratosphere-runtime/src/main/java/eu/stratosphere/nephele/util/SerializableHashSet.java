/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.util.StringUtils;

/**
 * This class extends a standard {@link java.util.HashSet} by implementing the
 * {@link eu.stratosphere.core.io.IOReadableWritable} interface. As a result, hash sets of this type can be used
 * with Nephele's RPC system.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type used in this hash set
 */
public class SerializableHashSet<T extends IOReadableWritable> extends HashSet<T> implements IOReadableWritable {

	/**
	 * The generated serial version UID.
	 */
	private static final long serialVersionUID = -4615823301768215807L;


	@Override
	public void write(final DataOutput out) throws IOException {

		out.writeInt(size());

		final Iterator<T> it = iterator();

		while (it.hasNext()) {

			final T entry = it.next();
			StringRecord.writeString(out, entry.getClass().getName());
			entry.write(out);
		}
	}


	@SuppressWarnings("unchecked")
	// TODO: See if type safety can be improved here
	@Override
	public void read(final DataInput in) throws IOException {

		final int numberOfMapEntries = in.readInt();

		for (int i = 0; i < numberOfMapEntries; i++) {

			final String type = StringRecord.readString(in);
			Class<T> clazz = null;
			try {
				clazz = (Class<T>) Class.forName(type);
			} catch (ClassNotFoundException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			T entry = null;
			try {
				entry = clazz.newInstance();
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			entry.read(in);

			add(entry);
		}

	}

}

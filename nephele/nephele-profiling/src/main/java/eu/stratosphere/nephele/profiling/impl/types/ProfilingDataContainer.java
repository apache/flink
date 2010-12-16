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

package eu.stratosphere.nephele.profiling.impl.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

public class ProfilingDataContainer implements IOReadableWritable {

	private final Queue<InternalProfilingData> queuedProfilingData = new ArrayDeque<InternalProfilingData>();

	public void addProfilingData(InternalProfilingData profilingData) {

		if (profilingData == null) {
			return;
		}

		this.queuedProfilingData.add(profilingData);
	}

	public void clear() {
		this.queuedProfilingData.clear();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInput in) throws IOException {

		final int numberOfRecords = in.readInt();
		for (int i = 0; i < numberOfRecords; i++) {
			final String className = StringRecord.readString(in);
			Class<? extends InternalProfilingData> clazz = null;

			try {
				clazz = (Class<? extends InternalProfilingData>) Class.forName(className);
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			InternalProfilingData profilingData = null;
			try {
				profilingData = clazz.newInstance();
			} catch (Exception e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			// Restore internal state
			profilingData.read(in);

			this.queuedProfilingData.add(profilingData);
		}
	}

	public int size() {
		return this.queuedProfilingData.size();
	}

	public boolean isEmpty() {
		return this.queuedProfilingData.isEmpty();
	}

	public Iterator<InternalProfilingData> getIterator() {
		return this.queuedProfilingData.iterator();
	}

	@Override
	public void write(DataOutput out) throws IOException {

		// Write the number of records
		out.writeInt(this.queuedProfilingData.size());
		// Write the records themselves
		final Iterator<InternalProfilingData> iterator = this.queuedProfilingData.iterator();
		while (iterator.hasNext()) {
			final InternalProfilingData profilingData = iterator.next();
			StringRecord.writeString(out, profilingData.getClass().getName());
			profilingData.write(out);
		}
	}

}

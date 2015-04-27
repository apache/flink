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

package org.apache.flink.runtime.profiling.impl.types;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.StringUtils;

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
	public void read(DataInputView in) throws IOException {

		final int numberOfRecords = in.readInt();
		for (int i = 0; i < numberOfRecords; i++) {
			final String className = StringUtils.readNullableString(in);

			Class<? extends InternalProfilingData> clazz;
			try {
				clazz = (Class<? extends InternalProfilingData>) Class.forName(className);
			} catch (Exception e) {
				throw new IOException(e);
			}

			InternalProfilingData profilingData ;
			try {
				profilingData = clazz.newInstance();
			} catch (Exception e) {
				throw new IOException(e);
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
	public void write(DataOutputView out) throws IOException {

		// Write the number of records
		out.writeInt(this.queuedProfilingData.size());
		// Write the records themselves
		for (InternalProfilingData profilingData : this.queuedProfilingData) {
			StringUtils.writeNullableString(profilingData.getClass().getName(), out);
			profilingData.write(out);
		}
	}

}

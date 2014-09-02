/**
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


package org.apache.flink.test.recordJobs.graph.pageRankUtil;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

public class LongArrayView implements Value {
	private static final long serialVersionUID = 1L;

	private long[] entries = new long[0];

	private int numEntries = 0;

	public LongArrayView() {
	}

	public long get(int index) {
		if (index >= numEntries) {
			throw new ArrayIndexOutOfBoundsException();
		}
		return getQuick(index);
	}

	public long getQuick(int index) {
		return entries[index];
	}

	public void allocate(int numEntries) {
		this.numEntries = numEntries;
		ensureCapacity();
	}

	public void set(int index, long value) {
		if (index >= numEntries) {
			throw new ArrayIndexOutOfBoundsException();
		}
		setQuick(index, value);
	}

	public void setQuick(int index, long value) {
		entries[index] = value;
	}

	public int size() {
		return numEntries;
	}

	private void ensureCapacity() {
		if (entries.length < numEntries) {
			entries = new long[numEntries];
		}
	}

	public void write(DataOutputView out) throws IOException {
		out.writeInt(numEntries);
		for (int n = 0; n < numEntries; n++) {
			out.writeLong(entries[n]);
		}
	}

	public void read(DataInputView in) throws IOException {
		numEntries = in.readInt();
		ensureCapacity();
		for (int n = 0; n < numEntries; n++) {
			entries[n] = in.readLong();
		}
	}
}

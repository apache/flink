/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.windowing.windows;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link Window} that represents a time interval from {@code start} (inclusive) to
 * {@code start + size} (exclusive).
 */
public class TimeWindow extends Window {

	private final long start;
	private final long end;

	public TimeWindow(long start, long end) {
		this.start = start;
		this.end = end;
	}

	public long getStart() {
		return start;
	}

	public long getEnd() {
		return end;
	}

	@Override
	public long maxTimestamp() {
		return end - 1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TimeWindow window = (TimeWindow) o;

		return end == window.end && start == window.start;
	}

	@Override
	public int hashCode() {
		int result = (int) (start ^ (start >>> 32));
		result = 31 * result + (int) (end ^ (end >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "TimeWindow{" +
				"start=" + start +
				", end=" + end +
				'}';
	}

	public static class Serializer extends TypeSerializer<TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public TypeSerializer<TimeWindow> duplicate() {
			return this;
		}

		@Override
		public TimeWindow createInstance() {
			return null;
		}

		@Override
		public TimeWindow copy(TimeWindow from) {
			return from;
		}

		@Override
		public TimeWindow copy(TimeWindow from, TimeWindow reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(TimeWindow record, DataOutputView target) throws IOException {
			target.writeLong(record.start);
			target.writeLong(record.end);
		}

		@Override
		public TimeWindow deserialize(DataInputView source) throws IOException {
			long start = source.readLong();
			long end = source.readLong();
			return new TimeWindow(start, end);
		}

		@Override
		public TimeWindow deserialize(TimeWindow reuse, DataInputView source) throws IOException {
			long start = source.readLong();
			long end = source.readLong();
			return new TimeWindow(start, end);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeLong(source.readLong());
			target.writeLong(source.readLong());
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof Serializer;
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof Serializer;
		}

		@Override
		public int hashCode() {
			return 0;
		}
	}

}

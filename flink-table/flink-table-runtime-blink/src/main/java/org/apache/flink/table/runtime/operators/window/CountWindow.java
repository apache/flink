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

package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.MathUtils;

import java.io.IOException;

/**
 * A {@link Window} that represents a count window. For each count window, we will assign a unique
 * id. Thus this CountWindow can act as namespace part in state. We can attach data to each
 * different CountWindow.
 */
public class CountWindow extends Window {

	private final long id;

	public CountWindow(long id) {
		this.id = id;
	}

	/**
	 * Gets the id (0-based) of the window.
	 */
	public long getId() {
		return id;
	}

	@Override
	public long maxTimestamp() {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CountWindow window = (CountWindow) o;

		return id == window.id;
	}

	@Override
	public int hashCode() {
		return MathUtils.longToIntWithBitMixing(id);
	}

	@Override
	public String toString() {
		return "CountWindow{" +
			"id=" + id + '}';
	}

	@Override
	public int compareTo(Window o) {
		CountWindow that = (CountWindow) o;
		return Long.compare(this.id, that.id);
	}

	// ------------------------------------------------------------------------
	// Serializer
	// ------------------------------------------------------------------------

	/**
	 * The serializer used to write the CountWindow type.
	 */
	public static class Serializer extends TypeSerializerSingleton<CountWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public CountWindow createInstance() {
			return null;
		}

		@Override
		public CountWindow copy(CountWindow from) {
			return from;
		}

		@Override
		public CountWindow copy(CountWindow from, CountWindow reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(CountWindow record, DataOutputView target) throws IOException {
			target.writeLong(record.id);
		}

		@Override
		public CountWindow deserialize(DataInputView source) throws IOException {
			return new CountWindow(source.readLong());
		}

		@Override
		public CountWindow deserialize(CountWindow reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeLong(source.readLong());
		}


		// ------------------------------------------------------------------------

		@Override
		public TypeSerializerSnapshot<CountWindow> snapshotConfiguration() {
			return new CountWindow.Serializer.CountWindowSerializerSnapshot();
		}

		/**
		 * Serializer configuration snapshot for compatibility and format evolution.
		 */
		@SuppressWarnings("WeakerAccess")
		public static final class CountWindowSerializerSnapshot extends SimpleTypeSerializerSnapshot<CountWindow> {

			public CountWindowSerializerSnapshot() {
				super(CountWindow.Serializer::new);
			}
		}
	}
}

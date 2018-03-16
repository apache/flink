/*
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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * The default window into which all data is placed (via
 * {@link org.apache.flink.streaming.api.windowing.assigners.GlobalWindows}).
 */
@PublicEvolving
public class GlobalWindow extends Window {

	private static final GlobalWindow INSTANCE = new GlobalWindow();

	private GlobalWindow() { }

	public static GlobalWindow get() {
		return INSTANCE;
	}

	@Override
	public long maxTimestamp() {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean equals(Object o) {
		return this == o || !(o == null || getClass() != o.getClass());
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public String toString() {
		return "GlobalWindow";
	}

	/**
	 * A {@link TypeSerializer} for {@link GlobalWindow}.
	 */
	public static class Serializer extends TypeSerializerSingleton<GlobalWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public GlobalWindow createInstance() {
			return GlobalWindow.INSTANCE;
		}

		@Override
		public GlobalWindow copy(GlobalWindow from) {
			return from;
		}

		@Override
		public GlobalWindow copy(GlobalWindow from, GlobalWindow reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(GlobalWindow record, DataOutputView target) throws IOException {
			target.writeByte(0);
		}

		@Override
		public GlobalWindow deserialize(DataInputView source) throws IOException {
			source.readByte();
			return GlobalWindow.INSTANCE;
		}

		@Override
		public GlobalWindow deserialize(GlobalWindow reuse,
				DataInputView source) throws IOException {
			source.readByte();
			return GlobalWindow.INSTANCE;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			source.readByte();
			target.writeByte(0);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof Serializer;
		}
	}
}

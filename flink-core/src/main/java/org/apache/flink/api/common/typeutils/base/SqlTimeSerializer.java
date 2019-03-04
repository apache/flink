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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;
import java.sql.Time;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

@Internal
public final class SqlTimeSerializer extends TypeSerializerSingleton<Time> {

	private static final long serialVersionUID = 1L;

	public static final SqlTimeSerializer INSTANCE = new SqlTimeSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public Time createInstance() {
		return new Time(0L);
	}

	@Override
	public Time copy(Time from) {
		if (from == null) {
			return null;
		}
		return new Time(from.getTime());
	}

	@Override
	public Time copy(Time from, Time reuse) {
		if (from == null) {
			return null;
		}
		reuse.setTime(from.getTime());
		return reuse;
	}

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(Time record, DataOutputView target) throws IOException {
		if (record == null) {
			target.writeLong(Long.MIN_VALUE);
		} else {
			target.writeLong(record.getTime());
		}
	}

	@Override
	public Time deserialize(DataInputView source) throws IOException {
		final long v = source.readLong();
		if (v == Long.MIN_VALUE) {
			return null;
		} else {
			return new Time(v);
		}
	}

	@Override
	public Time deserialize(Time reuse, DataInputView source) throws IOException {
		final long v = source.readLong();
		if (v == Long.MIN_VALUE) {
			return null;
		}
		reuse.setTime(v);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeLong(source.readLong());
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<Time> snapshotConfiguration() {
		return new SqlTimeSerializerSnapshot();
	}


	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class SqlTimeSerializerSnapshot extends SimpleTypeSerializerSnapshot<Time> {

		public SqlTimeSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}

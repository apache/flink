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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Internal
public final class LocalDateTimeSerializer extends TypeSerializerSingleton<LocalDateTime> {

	private static final long serialVersionUID = 1L;

	public static final LocalDateTimeSerializer INSTANCE = new LocalDateTimeSerializer();

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public LocalDateTime createInstance() {
		return LocalDateTime.of(
				LocalDateSerializer.INSTANCE.createInstance(),
				LocalTimeSerializer.INSTANCE.createInstance());
	}

	@Override
	public LocalDateTime copy(LocalDateTime from) {
		return from;
	}

	@Override
	public LocalDateTime copy(LocalDateTime from, LocalDateTime reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return LocalDateSerializer.INSTANCE.getLength() + LocalTimeSerializer.INSTANCE.getLength();
	}

	@Override
	public void serialize(LocalDateTime record, DataOutputView target) throws IOException {
		if (record == null) {
			LocalDateSerializer.INSTANCE.serialize(null, target);
			LocalTimeSerializer.INSTANCE.serialize(null, target);
		} else {
			LocalDateSerializer.INSTANCE.serialize(record.toLocalDate(), target);
			LocalTimeSerializer.INSTANCE.serialize(record.toLocalTime(), target);
		}
	}

	@Override
	public LocalDateTime deserialize(DataInputView source) throws IOException {
		LocalDate localDate = LocalDateSerializer.INSTANCE.deserialize(source);
		LocalTime localTime = LocalTimeSerializer.INSTANCE.deserialize(source);
		if (localDate == null && localTime == null) {
			return null;
		} else if (localDate == null || localTime == null) {
			throw new IOException("Exactly one of LocalDate and LocalTime is null.");
		} else {
			return LocalDateTime.of(localDate, localTime);
		}
	}

	@Override
	public LocalDateTime deserialize(LocalDateTime reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		LocalDateSerializer.INSTANCE.copy(source, target);
		LocalTimeSerializer.INSTANCE.copy(source, target);
	}

	@Override
	public TypeSerializerSnapshot<LocalDateTime> snapshotConfiguration() {
		return new LocalDateTimeSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class LocalDateTimeSerializerSnapshot extends SimpleTypeSerializerSnapshot<LocalDateTime> {

		public LocalDateTimeSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}

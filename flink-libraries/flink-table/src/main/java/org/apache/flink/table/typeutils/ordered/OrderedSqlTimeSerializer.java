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

package org.apache.flink.table.typeutils.ordered;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.sql.Time;

/**
 * A serializer for Time. The serialized value maintains the sort order of the original value.
 */
@Internal
public final class OrderedSqlTimeSerializer extends TypeSerializerSingleton<Time> {

	private static final long serialVersionUID = 1L;

	public static final OrderedSqlTimeSerializer ASC_INSTANCE =
		new OrderedSqlTimeSerializer(OrderedBytes.Order.ASCENDING);

	public static final OrderedSqlTimeSerializer DESC_INSTANCE =
		new OrderedSqlTimeSerializer(OrderedBytes.Order.DESCENDING);

	private static final OrderedBytes orderedBytes = new OrderedBytes();

	private final OrderedBytes.Order ord;

	private OrderedSqlTimeSerializer(OrderedBytes.Order ord) {
		this.ord = ord;
	}

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
			throw new IllegalArgumentException("The record must not be null.");
		}

		orderedBytes.encodeLong(target, record.getTime(), ord);
	}

	@Override
	public Time deserialize(DataInputView source) throws IOException {
		final long v = orderedBytes.decodeLong(source, ord);
		return new Time(v);
	}

	@Override
	public Time deserialize(Time reuse, DataInputView source) throws IOException {
		final long v = orderedBytes.decodeLong(source, ord);
		reuse.setTime(v);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		orderedBytes.encodeLong(target, orderedBytes.decodeLong(source, ord), ord);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedSqlTimeSerializer;
	}
}

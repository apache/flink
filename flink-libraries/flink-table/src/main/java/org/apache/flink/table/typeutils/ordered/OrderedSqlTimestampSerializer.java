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
import java.sql.Timestamp;

/**
 * A serializer for Timestamp. The serialized value maintains the sort order of the original value.
 */
@Internal
public final class OrderedSqlTimestampSerializer extends TypeSerializerSingleton<Timestamp> {

	private static final long serialVersionUID = 1L;

	public static final OrderedSqlTimestampSerializer ASC_INSTANCE =
		new OrderedSqlTimestampSerializer(OrderedBytes.Order.ASCENDING);

	public static final OrderedSqlTimestampSerializer DESC_INSTANCE =
		new OrderedSqlTimestampSerializer(OrderedBytes.Order.DESCENDING);

	private static final OrderedBytes orderedBytes = new OrderedBytes();

	private final OrderedBytes.Order ord;

	private OrderedSqlTimestampSerializer(OrderedBytes.Order ord) {
		this.ord = ord;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public Timestamp createInstance() {
		return new Timestamp(0L);
	}

	@Override
	public Timestamp copy(Timestamp from) {
		return new Timestamp(from.getTime());
	}

	@Override
	public Timestamp copy(Timestamp from, Timestamp reuse) {
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
	public void serialize(Timestamp record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The record must not be null.");
		}

		orderedBytes.encodeLong(target, record.getTime(), ord);
	}

	@Override
	public Timestamp deserialize(DataInputView source) throws IOException {
		final long v = orderedBytes.decodeLong(source, ord);

		return new Timestamp(v);
	}

	@Override
	public Timestamp deserialize(Timestamp reuse, DataInputView source) throws IOException {
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
		return obj instanceof OrderedSqlTimestampSerializer;
	}
}

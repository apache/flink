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

/**
 * A serializer for short. The serialized value maintains the sort order of the original value.
 */
@Internal
public final class OrderedShortSerializer extends TypeSerializerSingleton<Short> {

	private static final long serialVersionUID = 1L;

	public static final OrderedShortSerializer ASC_INSTANCE = new OrderedShortSerializer(OrderedBytes.Order.ASCENDING);

	public static final OrderedShortSerializer DESC_INSTANCE =
		new OrderedShortSerializer(OrderedBytes.Order.DESCENDING);

	private static final Short ZERO = (short) 0;

	private static final OrderedBytes orderedBytes = new OrderedBytes();

	private final OrderedBytes.Order ord;

	private OrderedShortSerializer(OrderedBytes.Order ord) {
		this.ord = ord;
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Short createInstance() {
		return ZERO;
	}

	@Override
	public Short copy(Short from) {
		return from;
	}

	@Override
	public Short copy(Short from, Short reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 2;
	}

	@Override
	public void serialize(Short record, DataOutputView target) throws IOException {
		orderedBytes.encodeShort(target, record, ord);
	}

	@Override
	public Short deserialize(DataInputView source) throws IOException {
		return orderedBytes.decodeShort(source, ord);
	}

	@Override
	public Short deserialize(Short reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		orderedBytes.encodeShort(target, orderedBytes.decodeShort(source, ord), ord);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedShortSerializer;
	}
}

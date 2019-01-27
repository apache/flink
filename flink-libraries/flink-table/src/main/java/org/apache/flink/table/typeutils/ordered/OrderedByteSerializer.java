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
 * A serializer for byte. The serialized value maintains the sort order of the original value.
 */
@Internal
public final class OrderedByteSerializer extends TypeSerializerSingleton<Byte> {

	private static final long serialVersionUID = 1L;

	public static final OrderedByteSerializer ASC_INSTANCE = new OrderedByteSerializer(OrderedBytes.Order.ASCENDING);

	public static final OrderedByteSerializer DESC_INSTANCE = new OrderedByteSerializer(OrderedBytes.Order.DESCENDING);

	private static final Byte ZERO = (byte) 0;

	private static final OrderedBytes orderedBytes = new OrderedBytes();

	private final OrderedBytes.Order ord;

	private OrderedByteSerializer(OrderedBytes.Order ord) {
		this.ord = ord;
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Byte createInstance() {
		return ZERO;
	}

	@Override
	public Byte copy(Byte from) {
		return from;
	}

	@Override
	public Byte copy(Byte from, Byte reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 1;
	}

	@Override
	public void serialize(Byte record, DataOutputView target) throws IOException {
		orderedBytes.encodeByte(target, record, ord);
	}

	@Override
	public Byte deserialize(DataInputView source) throws IOException {
		return orderedBytes.decodeByte(source, ord);
	}

	@Override
	public Byte deserialize(Byte reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		orderedBytes.encodeByte(target, orderedBytes.decodeByte(source, ord), ord);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedByteSerializer;
	}
}

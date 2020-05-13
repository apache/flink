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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RawType;

/**
 * Converter for {@link RawType} of {@code byte[]} external type.
 */
@Internal
class RawByteArrayConverter<T> implements DataStructureConverter<RawValueData<T>, byte[]> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> serializer;

	private RawByteArrayConverter(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public RawValueData<T> toInternal(byte[] external) {
		return RawValueData.fromBytes(external);
	}

	@Override
	public byte[] toExternal(RawValueData<T> internal) {
		return internal.toBytes(serializer);
	}

	// --------------------------------------------------------------------------------------------
	// Factory method
	// --------------------------------------------------------------------------------------------

	public static RawByteArrayConverter<?> create(DataType dataType) {
		final TypeSerializer<?> serializer = ((RawType<?>) dataType.getLogicalType()).getTypeSerializer();
		return new RawByteArrayConverter<>(serializer);
	}
}

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

package org.apache.flink.table.format.single;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * SingleValueSerializer: deserialize or serialize single value.
 */
public class SingleValueRowSerializer implements DeserializationSchema<RowData>,
	SerializationSchema<RowData> {

	private TypeInformation<RowData> typeInfo;
	private LogicalType singleValueLogicalType;
	private SingleValueSerializer singleValueSerializer;
	private FieldGetter fieldGetter;

	public SingleValueRowSerializer(RowType rowType,
		TypeInformation<RowData> resultTypeInfo) {
		this.typeInfo = resultTypeInfo;
		this.singleValueLogicalType = rowType.getTypeAt(0);
		this.fieldGetter = RowData.createFieldGetter(singleValueLogicalType, 0);
		this.singleValueSerializer = initSingleValueSerializer(singleValueLogicalType.getTypeRoot());
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		GenericRowData genericRowData = new GenericRowData(1);
		genericRowData.setField(0, singleValueSerializer.deserialize(message));
		return genericRowData;
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return typeInfo;
	}

	@Override
	public byte[] serialize(RowData element) {
		return singleValueSerializer.serialize(fieldGetter.getFieldOrNull(element));
	}

	private SingleValueSerializer initSingleValueSerializer(LogicalTypeRoot typeRoot) {
		switch (typeRoot) {
			case CHAR:
			case VARCHAR:
				return new StringSingleValueSerializer();
			case VARBINARY:
			case BINARY:
				return new BytesSingleValueSerializer();
			case TINYINT:
				return new BasicSingleValueSerializer(ByteSerializer.INSTANCE, BasicTypeInfo.BYTE_TYPE_INFO);
			case SMALLINT:
				return new BasicSingleValueSerializer(ShortSerializer.INSTANCE, BasicTypeInfo.SHORT_TYPE_INFO);
			case INTEGER:
				return new BasicSingleValueSerializer(IntSerializer.INSTANCE, BasicTypeInfo.INT_TYPE_INFO);
			case BIGINT:
				return new BasicSingleValueSerializer(LongSerializer.INSTANCE, BasicTypeInfo.LONG_TYPE_INFO);
			case FLOAT:
				return new BasicSingleValueSerializer(FloatSerializer.INSTANCE, BasicTypeInfo.FLOAT_TYPE_INFO);
			case DOUBLE:
				return new BasicSingleValueSerializer(DoubleSerializer.INSTANCE, BasicTypeInfo.DOUBLE_TYPE_INFO);
			case BOOLEAN:
				return new BasicSingleValueSerializer(BooleanSerializer.INSTANCE, BasicTypeInfo.BOOLEAN_TYPE_INFO);
			default:
				throw new RuntimeException("Unsupported single value type:" + singleValueLogicalType.getTypeRoot());
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SingleValueRowSerializer that = (SingleValueRowSerializer) o;
		return typeInfo.equals(that.typeInfo);
	}
}

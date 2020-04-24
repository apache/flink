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

package org.apache.flink.formats.single.value;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.formats.single.value.serializer.BasicValueSerializer;
import org.apache.flink.formats.single.value.serializer.BytesSerializer;
import org.apache.flink.formats.single.value.serializer.SingleValueSerializer;
import org.apache.flink.formats.single.value.serializer.StringSerializer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * SingleValueSerializer: deserialize or serialize single value.
 */
public class SingleValueRowSerializer implements DeserializationSchema<Row>,
	SerializationSchema<Row> {

	private TypeInformation<Row> typeInfo;
	private DataType singleValueDataType;
	private SingleValueSerializer singleValueSerializer;

	public SingleValueRowSerializer(TableSchema tableSchema) {
		this.typeInfo = tableSchema.toRowType();
		this.singleValueDataType = tableSchema.getFieldDataType(0)
			.orElseThrow(() -> new RuntimeException("Invalid table schema"));

		Class conversionClass = singleValueDataType.getConversionClass();
		if (String.class.equals(conversionClass)) {
			this.singleValueSerializer = new StringSerializer();
		} else if (byte[].class.equals(conversionClass)) {
			this.singleValueSerializer = new BytesSerializer();
		} else if (Short.class.equals(conversionClass)) {
			this.singleValueSerializer = new BasicValueSerializer(ShortSerializer.INSTANCE, BasicTypeInfo.SHORT_TYPE_INFO);
		} else if (Integer.class.getName().equals(conversionClass.getName())) {
			this.singleValueSerializer = new BasicValueSerializer(IntSerializer.INSTANCE, BasicTypeInfo.INT_TYPE_INFO);
		} else if (Long.class.equals(conversionClass)) {
			this.singleValueSerializer = new BasicValueSerializer(LongSerializer.INSTANCE, BasicTypeInfo.LONG_TYPE_INFO);
		} else if (Float.class.equals(conversionClass)) {
			this.singleValueSerializer = new BasicValueSerializer(FloatSerializer.INSTANCE, BasicTypeInfo.FLOAT_TYPE_INFO);
		} else if (Double.class.equals(conversionClass)) {
			this.singleValueSerializer = new BasicValueSerializer(DoubleSerializer.INSTANCE, BasicTypeInfo.DOUBLE_TYPE_INFO);
		} else if (Boolean.class.equals(conversionClass)) {
			this.singleValueSerializer = new BasicValueSerializer(BooleanSerializer.INSTANCE, BasicTypeInfo.BOOLEAN_TYPE_INFO);
		} else if (Character.class.equals(conversionClass)) {
			this.singleValueSerializer = new BasicValueSerializer(CharSerializer.INSTANCE, BasicTypeInfo.CHAR_TYPE_INFO);
		} else {
			throw new RuntimeException("Unsupported single value type:" + singleValueDataType
				+ " with conversion class:" + conversionClass);
		}
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		return Row.of(singleValueSerializer.deserialize(message));
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	@Override
	public byte[] serialize(Row element) {
		return singleValueSerializer.serialize(element.getField(0));
	}
}

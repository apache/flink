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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.thrift.typeutils.ThriftUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 *  ThriftRowSerializationSchema serializes @Row objects based on TProtocol configuration.
 */
public class ThriftRowSerializationSchema implements SerializationSchema<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(ThriftRowSerializationSchema.class);

	/** Type information describing the input type. */
	private final Class<? extends TBase> thriftClazz;

	private TableSchema schema;

	private TSerializer tSerializer;

	public ThriftRowSerializationSchema(Class<? extends TBase> thriftClazz, TableSchema schema,
		Class<? extends TProtocolFactory> tProtocolFactoryClass) {
		Preconditions.checkNotNull(thriftClazz, "Thrift record class must not be null.");
		this.thriftClazz = thriftClazz;
		this.schema = schema;
		try {
			this.tSerializer = new TSerializer(tProtocolFactoryClass.newInstance());
		} catch (Exception e) {
		}
	}

	@Override
	public byte[] serialize(Row row) {
		byte[] result = null;
		try {
			TBase thriftObject = thriftClazz.newInstance();
			int numFields = row.getArity();
			String[] fieldNames = schema.getFieldNames();
			for (int i = 0; i < numFields; i++) {
				TFieldIdEnum fieldIdEnum = ThriftUtils.getFieldIdEnum(thriftClazz, fieldNames[i]);
				thriftObject.setFieldValue(fieldIdEnum, row.getField(i));
			}
			result = tSerializer.serialize(thriftObject);
		} catch (Exception e) {
			LOG.error("Failed to serialize row {}", row, e);
		}
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final ThriftRowSerializationSchema that = (ThriftRowSerializationSchema) o;
		return Objects.equals(thriftClazz, that.thriftClazz);
	}

	@Override
	public int hashCode() {
		return Objects.hash(thriftClazz);
	}
}

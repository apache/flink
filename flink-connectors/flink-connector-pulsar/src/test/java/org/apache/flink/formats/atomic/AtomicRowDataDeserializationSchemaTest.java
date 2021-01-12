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

package org.apache.flink.formats.atomic;

import org.apache.flink.streaming.connectors.pulsar.util.RowDataUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TestLogger;

import org.apache.pulsar.client.api.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.junit.Assert.assertEquals;

/**
 * test for AtomicRowDataDeserializationSchema.
 */
@RunWith(Parameterized.class)
public class AtomicRowDataDeserializationSchemaTest extends TestLogger {

	@Parameterized.Parameter(0)
	public Schema schema;
	@Parameterized.Parameter(1)
	public Object expectedValue;

	@Parameterized.Parameters
	public static Object[][] data() {
		return new Object[][]{
			new Object[]{
				Schema.INT8,
				Byte.MAX_VALUE
			},
			new Object[]{
				Schema.INT16,
				Short.MAX_VALUE
			},
			new Object[]{
				Schema.INT32,
				Integer.MAX_VALUE
			},
			new Object[]{
				Schema.INT64,
				Long.MAX_VALUE
			},
			new Object[]{
				Schema.DOUBLE,
				Double.MAX_VALUE
			},
			new Object[]{
				Schema.FLOAT,
				Float.MAX_VALUE
			},
			new Object[]{
				Schema.BYTES,
				new byte[]{1, 2, 3, 5, 6, 8, 9, 0}
			},
			new Object[]{
				Schema.STRING,
				"text"
			},
//                new Object[]{
//                        Schema.DATE,
//                        Date.from(Instant.now())
//                },
			new Object[]{
				Schema.LOCAL_DATE,
				LocalDate.now()
			},
			new Object[]{
				Schema.LOCAL_DATE_TIME,
				LocalDateTime.now()
			},
			new Object[]{
				Schema.LOCAL_TIME,
				LocalTime.now()
			},
			new Object[]{
				Schema.INSTANT,
				Instant.now()
			},
		};
	}

	@Test
	public void deserialize() throws IOException {
		final AtomicRowDataDeserializationSchema deserializationSchema =
			new AtomicRowDataDeserializationSchema.Builder(expectedValue
				.getClass()
				.getName()).build();
		final byte[] encode = schema.encode(expectedValue);
		final RowData rowData = deserializationSchema.deserialize(encode);
		final Object value = RowDataUtil.getField(rowData, 0, expectedValue.getClass());
		assertEquals(expectedValue, value);
	}

	@Test
	public void serialize() throws IOException {
		final AtomicRowDataSerializationSchema serializationSchema =
			new AtomicRowDataSerializationSchema.Builder(expectedValue
				.getClass()
				.getName()).build();
		final GenericRowData rowData = new GenericRowData(RowKind.INSERT, 1);
		RowDataUtil.setField(rowData, 0, expectedValue);
		final byte[] encode = serializationSchema.serialize(rowData);
		final Object value = schema.decode(encode);
		assertEquals(expectedValue, value);
	}
}

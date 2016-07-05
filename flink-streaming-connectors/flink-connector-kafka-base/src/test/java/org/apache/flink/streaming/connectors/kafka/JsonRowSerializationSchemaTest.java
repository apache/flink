/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.table.Row;
import org.apache.flink.streaming.util.serialization.JsonRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JsonRowSerializationSchemaTest {
	@Test
	public void testRowSerialization() throws IOException {
		String[] fieldNames = new String[] {"f1", "f2", "f3"};
		Class[] fieldTypes = new Class[] {Integer.class, Boolean.class, String.class};
		Row row = new Row(3);
		row.setField(0, 1);
		row.setField(1, true);
		row.setField(2, "str");


		JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(fieldNames);
		JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(fieldNames, fieldTypes);

		byte[] bytes = serializationSchema.serialize(row);
		Row resultRow = deserializationSchema.deserialize(bytes);

		assertEquals("Deserialized row should have expected number of fields",
			row.getFieldNumber(), resultRow.getFieldNumber());
		for (int i = 0; i < row.getFieldNumber(); i++) {
			assertEquals(String.format("Field number %d should be as in the original row", i),
				row.getField(i), resultRow.getField(i));
		}
	}
}

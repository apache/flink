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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Abstract test base for all Kafka JSON table sources.
 *
 * @deprecated Ensures backwards compatibility with Flink 1.5. Can be removed once we
 *             drop support for format-specific table sources.
 */
@Deprecated
public abstract class KafkaJsonTableSourceTestBase extends KafkaTableSourceBuilderTestBase {

	@Test
	public void testJsonEqualsTableSchema() {
		KafkaJsonTableSource.Builder b = (KafkaJsonTableSource.Builder) getBuilder();
		this.configureBuilder(b);

		KafkaJsonTableSource source = (KafkaJsonTableSource) b.build();

		// check return type
		RowTypeInfo returnType = (RowTypeInfo) source.getReturnType();
		assertNotNull(returnType);
		assertEquals(5, returnType.getArity());
		// check field names
		assertEquals("field1", returnType.getFieldNames()[0]);
		assertEquals("field2", returnType.getFieldNames()[1]);
		assertEquals("time1", returnType.getFieldNames()[2]);
		assertEquals("time2", returnType.getFieldNames()[3]);
		assertEquals("field3", returnType.getFieldNames()[4]);
		// check field types
		assertEquals(Types.LONG(), returnType.getTypeAt(0));
		assertEquals(Types.STRING(), returnType.getTypeAt(1));
		assertEquals(Types.SQL_TIMESTAMP(), returnType.getTypeAt(2));
		assertEquals(Types.SQL_TIMESTAMP(), returnType.getTypeAt(3));
		assertEquals(Types.DOUBLE(), returnType.getTypeAt(4));

		// check field mapping
		assertNull(source.getFieldMapping());
	}

	@Test
	public void testCustomJsonSchemaWithMapping() {
		KafkaJsonTableSource.Builder b = (KafkaJsonTableSource.Builder) getBuilder();
		super.configureBuilder(b);
		b.withProctimeAttribute("time2");

		Map<String, String> mapping = new HashMap<>();
		mapping.put("field1", "otherField1");
		mapping.put("field2", "otherField2");
		mapping.put("field3", "otherField3");

		// set Avro class with different fields
		b.forJsonSchema(TableSchema.builder()
			.field("otherField1", Types.LONG())
			.field("otherField2", Types.STRING())
			.field("rowtime", Types.LONG())
			.field("otherField3", Types.DOUBLE())
			.field("otherField4", Types.BYTE())
			.field("otherField5", Types.INT()).build());
		b.withTableToJsonMapping(mapping);
		b.withRowtimeAttribute("time1", new ExistingField("timeField1"), new AscendingTimestamps());

		KafkaJsonTableSource source = (KafkaJsonTableSource) b.build();

		// check return type
		RowTypeInfo returnType = (RowTypeInfo) source.getReturnType();
		assertNotNull(returnType);
		assertEquals(6, returnType.getArity());
		// check field names
		assertEquals("otherField1", returnType.getFieldNames()[0]);
		assertEquals("otherField2", returnType.getFieldNames()[1]);
		assertEquals("rowtime", returnType.getFieldNames()[2]);
		assertEquals("otherField3", returnType.getFieldNames()[3]);
		assertEquals("otherField4", returnType.getFieldNames()[4]);
		assertEquals("otherField5", returnType.getFieldNames()[5]);
		// check field types
		assertEquals(Types.LONG(), returnType.getTypeAt(0));
		assertEquals(Types.STRING(), returnType.getTypeAt(1));
		assertEquals(Types.LONG(), returnType.getTypeAt(2));
		assertEquals(Types.DOUBLE(), returnType.getTypeAt(3));
		assertEquals(Types.BYTE(), returnType.getTypeAt(4));
		assertEquals(Types.INT(), returnType.getTypeAt(5));

		// check field mapping
		Map<String, String> fieldMapping = source.getFieldMapping();
		assertNotNull(fieldMapping);
		assertEquals(3, fieldMapping.size());
		assertEquals("otherField1", fieldMapping.get("field1"));
		assertEquals("otherField2", fieldMapping.get("field2"));
		assertEquals("otherField3", fieldMapping.get("field3"));
	}

}

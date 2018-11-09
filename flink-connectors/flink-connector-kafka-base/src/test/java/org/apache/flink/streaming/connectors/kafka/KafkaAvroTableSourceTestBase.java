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
import org.apache.flink.formats.avro.generated.DifferentSchemaRecord;
import org.apache.flink.formats.avro.generated.SchemaRecord;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Abstract test base for all Kafka Avro table sources.
 *
 * @deprecated Ensures backwards compatibility with Flink 1.5. Can be removed once we
 *             drop support for format-specific table sources.
 */
@Deprecated
public abstract class KafkaAvroTableSourceTestBase extends KafkaTableSourceBuilderTestBase {

	@Override
	protected void configureBuilder(KafkaTableSourceBase.Builder builder) {
		super.configureBuilder(builder);
		((KafkaAvroTableSource.Builder) builder).forAvroRecordClass(SchemaRecord.class);
	}

	@Test
	public void testSameFieldsAvroClass() {
		KafkaAvroTableSource.Builder b = (KafkaAvroTableSource.Builder) getBuilder();
		this.configureBuilder(b);

		KafkaAvroTableSource source = (KafkaAvroTableSource) b.build();

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
		assertEquals(Types.LONG(), returnType.getTypeAt(2));
		assertEquals(Types.LONG(), returnType.getTypeAt(3));
		assertEquals(Types.DOUBLE(), returnType.getTypeAt(4));

		// check field mapping
		assertNull(source.getFieldMapping());

		// check if DataStream type matches with TableSource.getReturnType()
		assertEquals(source.getReturnType(),
			source.getDataStream(StreamExecutionEnvironment.getExecutionEnvironment()).getType());
	}

	@Test
	public void testDifferentFieldsAvroClass() {
		KafkaAvroTableSource.Builder b = (KafkaAvroTableSource.Builder) getBuilder();
		super.configureBuilder(b);
		b.withProctimeAttribute("time2");

		Map<String, String> mapping = new HashMap<>();
		mapping.put("field1", "otherField1");
		mapping.put("field2", "otherField2");
		mapping.put("field3", "otherField3");

		// set Avro class with different fields
		b.forAvroRecordClass(DifferentSchemaRecord.class);
		b.withTableToAvroMapping(mapping);

		KafkaAvroTableSource source = (KafkaAvroTableSource) b.build();

		// check return type
		RowTypeInfo returnType = (RowTypeInfo) source.getReturnType();
		assertNotNull(returnType);
		assertEquals(6, returnType.getArity());
		// check field names
		assertEquals("otherField1", returnType.getFieldNames()[0]);
		assertEquals("otherField2", returnType.getFieldNames()[1]);
		assertEquals("otherTime1", returnType.getFieldNames()[2]);
		assertEquals("otherField3", returnType.getFieldNames()[3]);
		assertEquals("otherField4", returnType.getFieldNames()[4]);
		assertEquals("otherField5", returnType.getFieldNames()[5]);
		// check field types
		assertEquals(Types.LONG(), returnType.getTypeAt(0));
		assertEquals(Types.STRING(), returnType.getTypeAt(1));
		assertEquals(Types.LONG(), returnType.getTypeAt(2));
		assertEquals(Types.DOUBLE(), returnType.getTypeAt(3));
		assertEquals(Types.FLOAT(), returnType.getTypeAt(4));
		assertEquals(Types.INT(), returnType.getTypeAt(5));

		// check field mapping
		Map<String, String> fieldMapping = source.getFieldMapping();
		assertNotNull(fieldMapping);
		assertEquals(3, fieldMapping.size());
		assertEquals("otherField1", fieldMapping.get("field1"));
		assertEquals("otherField2", fieldMapping.get("field2"));
		assertEquals("otherField3", fieldMapping.get("field3"));

		// check if DataStream type matches with TableSource.getReturnType()
		assertEquals(source.getReturnType(),
			source.getDataStream(StreamExecutionEnvironment.getExecutionEnvironment()).getType());
	}
}

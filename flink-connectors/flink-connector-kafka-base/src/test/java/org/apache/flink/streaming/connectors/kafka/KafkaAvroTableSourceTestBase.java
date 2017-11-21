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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.table.api.Types;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Abstract test base for all Kafka Avro table sources.
 */
public abstract class KafkaAvroTableSourceTestBase extends KafkaTableSourceTestBase {

	@Override
	protected void configureBuilder(KafkaTableSource.Builder builder) {
		super.configureBuilder(builder);
		((KafkaAvroTableSource.Builder) builder).forAvroRecordClass(SameFieldsAvroClass.class);
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
		assertEquals(Types.SQL_TIMESTAMP(), returnType.getTypeAt(2));
		assertEquals(Types.SQL_TIMESTAMP(), returnType.getTypeAt(3));
		assertEquals(Types.DOUBLE(), returnType.getTypeAt(4));

		// check field mapping
		assertNull(source.getFieldMapping());
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
		b.forAvroRecordClass(DifferentFieldsAvroClass.class);
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
		assertEquals(Types.SQL_TIMESTAMP(), returnType.getTypeAt(2));
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

	/**
	 * Avro record that matches the table schema.
	 */
	@SuppressWarnings("unused")
	public static class SameFieldsAvroClass extends SpecificRecordBase {

		//CHECKSTYLE.OFF: StaticVariableNameCheck - Avro accesses this field by name via reflection.
		public static Schema SCHEMA$ = AvroTestUtils.createFlatAvroSchema(FIELD_NAMES, FIELD_TYPES);
		//CHECKSTYLE.ON: StaticVariableNameCheck

		public Long field1;
		public String field2;
		public Timestamp time1;
		public Timestamp time2;
		public Double field3;

		@Override
		public Schema getSchema() {
			return null;
		}

		@Override
		public Object get(int field) {
			return null;
		}

		@Override
		public void put(int field, Object value) { }
	}

	/**
	 * Avro record that does NOT match the table schema.
	 */
	@SuppressWarnings("unused")
	public static class DifferentFieldsAvroClass extends SpecificRecordBase {

		//CHECKSTYLE.OFF: StaticVariableNameCheck - Avro accesses this field by name via reflection.
		public static Schema SCHEMA$ = AvroTestUtils.createFlatAvroSchema(
			new String[]{"otherField1", "otherField2", "otherTime1", "otherField3", "otherField4", "otherField5"},
			new TypeInformation[]{Types.LONG(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.DOUBLE(), Types.BYTE(), Types.INT()});
		//CHECKSTYLE.ON: StaticVariableNameCheck

		public Long otherField1;
		public String otherField2;
		public Timestamp otherTime1;
		public Double otherField3;
		public Byte otherField4;
		public Integer otherField5;

		@Override
		public Schema getSchema() {
			return null;
		}

		@Override
		public Object get(int field) {
			return null;
		}

		@Override
		public void put(int field, Object value) { }
	}

}

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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.testutils.AvroTestUtils;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;

import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Abstract test base for all Kafka table sources.
 */
public abstract class KafkaTableSourceTestBase {

	private static final String TOPIC = "testTopic";
	private static final String[] FIELD_NAMES = new String[] { "mylong", "mystring", "myboolean", "mydouble", "missingField" };
	private static final TypeInformation<?>[] FIELD_TYPES = new TypeInformation<?>[] {
		BasicTypeInfo.LONG_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.BOOLEAN_TYPE_INFO,
		BasicTypeInfo.DOUBLE_TYPE_INFO,
		BasicTypeInfo.LONG_TYPE_INFO };
	private static final Properties PROPERTIES = createSourceProperties();

	/**
	 * Avro record that matches above schema.
	 */
	public static class AvroSpecificRecord extends SpecificRecordBase {

		//CHECKSTYLE.OFF: StaticVariableNameCheck - Avro accesses this field by name via reflection.
		public static Schema SCHEMA$ = AvroTestUtils.createFlatAvroSchema(FIELD_NAMES, FIELD_TYPES);
		//CHECKSTYLE.ON: StaticVariableNameCheck

		public Long mylong;
		public String mystring;
		public Boolean myboolean;
		public Double mydouble;
		public Long missingField;

		@Override
		public Schema getSchema() {
			return null;
		}

		@Override
		public Object get(int field) {
			return null;
		}

		@Override
		public void put(int field, Object value) {

		}
	}

	@Test
	public void testKafkaTableSource() {
		KafkaTableSource kafkaTableSource = spy(createTableSource());
		StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
		kafkaTableSource.getDataStream(env);

		verify(env).addSource(any(getFlinkKafkaConsumer()));

		verify(kafkaTableSource).getKafkaConsumer(
			eq(TOPIC),
			eq(PROPERTIES),
			any(getDeserializationSchema()));
	}

	protected abstract KafkaTableSource createTableSource(String topic, Properties properties, TypeInformation<Row> typeInfo);

	protected abstract Class<DeserializationSchema<Row>> getDeserializationSchema();

	protected abstract Class<FlinkKafkaConsumerBase<Row>> getFlinkKafkaConsumer();

	private KafkaTableSource createTableSource() {
		return createTableSource(TOPIC, PROPERTIES, Types.ROW(FIELD_NAMES, FIELD_TYPES));
	}

	private static Properties createSourceProperties() {
		Properties properties = new Properties();
		properties.setProperty("zookeeper.connect", "dummy");
		properties.setProperty("group.id", "dummy");
		return properties;
	}
}

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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Abstract test base for all format-specific Kafka table sources with builders.
 *
 * @deprecated Ensures backwards compatibility with Flink 1.5. Can be removed once we
 *             drop support for format-specific table sources.
 */
@Deprecated
public abstract class KafkaTableSourceBuilderTestBase {

	static final String[] FIELD_NAMES =
		new String[]{"field1", "field2", "time1", "time2", "field3"};
	static final TypeInformation[] FIELD_TYPES =
		new TypeInformation[]{Types.LONG(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.DOUBLE()};

	private static final String TOPIC = "testTopic";
	private static final TableSchema SCHEMA = new TableSchema(FIELD_NAMES, FIELD_TYPES);
	private static final Properties PROPS = createSourceProperties();

	@Test
	@SuppressWarnings("unchecked")
	public void testKafkaConsumer() {
		KafkaTableSource.Builder b = getBuilder();
		configureBuilder(b);

		// assert that correct
		KafkaTableSource observed = spy(b.build());
		StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
		when(env.addSource(any(SourceFunction.class))).thenReturn(mock(DataStreamSource.class));
		observed.getDataStream(env);

		verify(env).addSource(any(getFlinkKafkaConsumer()));

		verify(observed).getKafkaConsumer(
			eq(TOPIC),
			eq(PROPS),
			any(getDeserializationSchema()));
	}

	@Test
	public void testTableSchema() {
		KafkaTableSource.Builder b = getBuilder();
		configureBuilder(b);

		KafkaTableSource source = b.build();

		// check table schema
		TableSchema schema = source.getTableSchema();
		assertNotNull(schema);
		assertEquals(5, schema.getColumnNames().length);
		// check table fields
		assertEquals("field1", schema.getColumnNames()[0]);
		assertEquals("field2", schema.getColumnNames()[1]);
		assertEquals("time1", schema.getColumnNames()[2]);
		assertEquals("time2", schema.getColumnNames()[3]);
		assertEquals("field3", schema.getColumnNames()[4]);
		assertEquals(Types.LONG(), schema.getTypes()[0]);
		assertEquals(Types.STRING(), schema.getTypes()[1]);
		assertEquals(Types.SQL_TIMESTAMP(), schema.getTypes()[2]);
		assertEquals(Types.SQL_TIMESTAMP(), schema.getTypes()[3]);
		assertEquals(Types.DOUBLE(), schema.getTypes()[4]);
	}

	@Test
	public void testNoTimeAttributes() {
		KafkaTableSource.Builder b = getBuilder();
		configureBuilder(b);

		KafkaTableSource source = b.build();

		// assert no proctime
		assertNull(source.getProctimeAttribute());
		// assert no rowtime
		assertNotNull(source.getRowtimeAttributeDescriptors());
		assertTrue(source.getRowtimeAttributeDescriptors().isEmpty());
	}

	@Test
	public void testProctimeAttribute() {
		KafkaTableSource.Builder b = getBuilder();
		configureBuilder(b);
		b.withProctimeAttribute("time1");

		KafkaTableSource source = b.build();

		// assert correct proctime field
		assertEquals(source.getProctimeAttribute(), "time1");

		// assert no rowtime
		assertNotNull(source.getRowtimeAttributeDescriptors());
		assertTrue(source.getRowtimeAttributeDescriptors().isEmpty());
	}

	@Test
	public void testRowtimeAttribute() {
		KafkaTableSource.Builder b = getBuilder();
		configureBuilder(b);
		b.withRowtimeAttribute("time2", new ExistingField("time2"), new AscendingTimestamps());

		KafkaTableSource source = b.build();

		// assert no proctime
		assertNull(source.getProctimeAttribute());

		// assert correct rowtime descriptor
		List<RowtimeAttributeDescriptor> descs = source.getRowtimeAttributeDescriptors();
		assertNotNull(descs);
		assertEquals(1, descs.size());
		RowtimeAttributeDescriptor desc = descs.get(0);
		assertEquals("time2", desc.getAttributeName());
		// assert timestamp extractor
		assertTrue(desc.getTimestampExtractor() instanceof ExistingField);
		assertEquals(1, desc.getTimestampExtractor().getArgumentFields().length);
		assertEquals("time2", desc.getTimestampExtractor().getArgumentFields()[0]);
		// assert watermark strategy
		assertTrue(desc.getWatermarkStrategy() instanceof AscendingTimestamps);
	}

	@Test
	public void testRowtimeAttribute2() {
		KafkaTableSource.Builder b = getBuilder();
		configureBuilder(b);

		try {
			b.withKafkaTimestampAsRowtimeAttribute("time2", new AscendingTimestamps());

			KafkaTableSource source = b.build();

			// assert no proctime
			assertNull(source.getProctimeAttribute());

			// assert correct rowtime descriptor
			List<RowtimeAttributeDescriptor> descs = source.getRowtimeAttributeDescriptors();
			assertNotNull(descs);
			assertEquals(1, descs.size());
			RowtimeAttributeDescriptor desc = descs.get(0);
			assertEquals("time2", desc.getAttributeName());
			// assert timestamp extractor
			assertTrue(desc.getTimestampExtractor() instanceof StreamRecordTimestamp);
			assertTrue(desc.getTimestampExtractor().getArgumentFields().length == 0);
			// assert watermark strategy
			assertTrue(desc.getWatermarkStrategy() instanceof AscendingTimestamps);
		} catch (Exception e) {
			if (b.supportsKafkaTimestamps()) {
				// builder should support Kafka timestamps
				fail();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testConsumerOffsets() {
		KafkaTableSource.Builder b = getBuilder();
		configureBuilder(b);

		// test the default behavior
		KafkaTableSource source = spy(b.build());
		when(source.createKafkaConsumer(TOPIC, PROPS, null))
				.thenReturn(mock(getFlinkKafkaConsumer()));

		verify(source.getKafkaConsumer(TOPIC, PROPS, null)).setStartFromGroupOffsets();

		// test reading from earliest
		b.fromEarliest();
		source = spy(b.build());
		when(source.createKafkaConsumer(TOPIC, PROPS, null))
				.thenReturn(mock(getFlinkKafkaConsumer()));

		verify(source.getKafkaConsumer(TOPIC, PROPS, null)).setStartFromEarliest();

		// test reading from latest
		b.fromLatest();
		source = spy(b.build());
		when(source.createKafkaConsumer(TOPIC, PROPS, null))
				.thenReturn(mock(getFlinkKafkaConsumer()));
		verify(source.getKafkaConsumer(TOPIC, PROPS, null)).setStartFromLatest();

		// test reading from group offsets
		b.fromGroupOffsets();
		source = spy(b.build());
		when(source.createKafkaConsumer(TOPIC, PROPS, null))
				.thenReturn(mock(getFlinkKafkaConsumer()));
		verify(source.getKafkaConsumer(TOPIC, PROPS, null)).setStartFromGroupOffsets();

		// test reading from given offsets
		b.fromSpecificOffsets(mock(Map.class));
		source = spy(b.build());
		when(source.createKafkaConsumer(TOPIC, PROPS, null))
				.thenReturn(mock(getFlinkKafkaConsumer()));
		verify(source.getKafkaConsumer(TOPIC, PROPS, null)).setStartFromSpecificOffsets(any(Map.class));
	}

	protected abstract KafkaTableSource.Builder getBuilder();

	protected abstract Class<DeserializationSchema<Row>> getDeserializationSchema();

	protected abstract Class<FlinkKafkaConsumerBase<Row>> getFlinkKafkaConsumer();

	protected void configureBuilder(KafkaTableSource.Builder builder) {
		builder
			.forTopic(TOPIC)
			.withKafkaProperties(PROPS)
			.withSchema(SCHEMA);
	}

	private static Properties createSourceProperties() {
		Properties properties = new Properties();
		properties.setProperty("zookeeper.connect", "dummy");
		properties.setProperty("group.id", "dummy");
		return properties;
	}

}

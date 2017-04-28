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
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.junit.Test;

import java.io.Serializable;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public abstract class KafkaTableSinkTestBase {

	private static final String TOPIC = "testTopic";
	protected static final String[] FIELD_NAMES = new String[] {"field1", "field2"};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[] { Types.INT(), Types.STRING() };
	private static final KafkaPartitioner<Row> PARTITIONER = new CustomPartitioner();
	private static final Properties PROPERTIES = createSinkProperties();
	@SuppressWarnings("unchecked")
	private final FlinkKafkaProducerBase<Row> PRODUCER = new FlinkKafkaProducerBase<Row>(
		TOPIC, new KeyedSerializationSchemaWrapper(getSerializationSchema()), PROPERTIES, PARTITIONER) {

		@Override
		protected void flush() {}
	};

	@Test
	@SuppressWarnings("unchecked")
	public void testKafkaTableSink() throws Exception {
		DataStream dataStream = mock(DataStream.class);

		KafkaTableSink kafkaTableSink = spy(createTableSink());
		kafkaTableSink.emitDataStream(dataStream);

		verify(dataStream).addSink(eq(PRODUCER));

		verify(kafkaTableSink).createKafkaProducer(
			eq(TOPIC),
			eq(PROPERTIES),
			any(getSerializationSchema().getClass()),
			eq(PARTITIONER));
	}

	@Test
	public void testConfiguration() {
		KafkaTableSink kafkaTableSink = createTableSink();
		KafkaTableSink newKafkaTableSink = kafkaTableSink.configure(FIELD_NAMES, FIELD_TYPES);
		assertNotSame(kafkaTableSink, newKafkaTableSink);

		assertArrayEquals(FIELD_NAMES, newKafkaTableSink.getFieldNames());
		assertArrayEquals(FIELD_TYPES, newKafkaTableSink.getFieldTypes());
		assertEquals(new RowTypeInfo(FIELD_TYPES), newKafkaTableSink.getOutputType());
	}

	protected abstract KafkaTableSink createTableSink(String topic, Properties properties,
			KafkaPartitioner<Row> partitioner, FlinkKafkaProducerBase<Row> kafkaProducer);

	protected abstract SerializationSchema<Row> getSerializationSchema();

	private KafkaTableSink createTableSink() {
		return createTableSink(TOPIC, PROPERTIES, PARTITIONER, PRODUCER);
	}

	private static Properties createSinkProperties() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:12345");
		return properties;
	}

	private static class CustomPartitioner extends KafkaPartitioner<Row> implements Serializable {
		@Override
		public int partition(Row next, byte[] serializedKey, byte[] serializedValue, int numPartitions) {
			return 0;
		}
	}
}

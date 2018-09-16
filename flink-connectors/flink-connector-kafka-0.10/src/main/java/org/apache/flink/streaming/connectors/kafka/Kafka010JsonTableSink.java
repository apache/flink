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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * Kafka 0.10 {@link KafkaTableSink} that serializes data in JSON format.
 *
 * @deprecated Use the {@link org.apache.flink.table.descriptors.Kafka} descriptor together
 *             with descriptors for schema and format instead. Descriptors allow for
 *             implementation-agnostic definition of tables. See also
 *             {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}.
 */
@Deprecated
public class Kafka010JsonTableSink extends KafkaJsonTableSink {

	/**
	 * Creates {@link KafkaTableSink} to write table rows as JSON-encoded records to a Kafka 0.10
	 * topic with fixed partition assignment.
	 *
	 * <p>Each parallel TableSink instance will write its rows to a single Kafka partition.</p>
	 * <ul>
	 * <li>If the number of Kafka partitions is less than the number of sink instances, different
	 * sink instances will write to the same partition.</li>
	 * <li>If the number of Kafka partitions is higher than the number of sink instance, some
	 * Kafka partitions won't receive data.</li>
	 * </ul>
	 *
	 * @param topic topic in Kafka to which table is written
	 * @param properties properties to connect to Kafka
	 * @deprecated Use table descriptors instead of implementation-specific classes.
	 */
	@Deprecated
	public Kafka010JsonTableSink(String topic, Properties properties) {
		super(topic, properties, new FlinkFixedPartitioner<>());
	}

	/**
	 * Creates {@link KafkaTableSink} to write table rows as JSON-encoded records to a Kafka 0.10
	 * topic with custom partition assignment.
	 *
	 * @param topic topic in Kafka to which table is written
	 * @param properties properties to connect to Kafka
	 * @param partitioner Kafka partitioner
	 * @deprecated Use table descriptors instead of implementation-specific classes.
	 */
	@Deprecated
	public Kafka010JsonTableSink(String topic, Properties properties, FlinkKafkaPartitioner<Row> partitioner) {
		super(topic, properties, partitioner);
	}

	@Override
	protected FlinkKafkaProducerBase<Row> createKafkaProducer(
			String topic,
			Properties properties,
			SerializationSchema<Row> serializationSchema,
			Optional<FlinkKafkaPartitioner<Row>> partitioner) {
		return new FlinkKafkaProducer010<>(
			topic,
			serializationSchema,
			properties,
			partitioner.orElse(new FlinkFixedPartitioner<>()));
	}

	@Override
	protected Kafka010JsonTableSink createCopy() {
		return new Kafka010JsonTableSink(
			topic,
			properties,
			partitioner.orElse(new FlinkFixedPartitioner<>()));
	}
}

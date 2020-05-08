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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSink;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSourceSinkFactory;

/**
 * Test for {@link Kafka010TableSource} and {@link Kafka010TableSink} created
 * by {@link Kafka010TableSourceSinkFactory}.
 */
public class Kafka010SourceSinkFactoryTest extends KafkaSourceSinkFactoryTestBase {
	@Override
	protected String factoryIdentifier() {
		return Kafka010SourceSinkFactory.IDENTIFIER;
	}

	@Override
	protected Class<?> getExpectedConsumerClass() {
		return FlinkKafkaConsumer010.class;
	}

	@Override
	protected Class<?> getExpectedProducerClass() {
		return FlinkKafkaProducer010.class;
	}

	@Override
	protected Class<?> getExpectedScanSourceClass() {
		return Kafka010ScanSource.class;
	}

	@Override
	protected Class<?> getExpectedSinkClass() {
		return Kafka010Sink.class;
	}
}

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

import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Tests for our own {@link FlinkKafkaProducer}.
 */
@SuppressWarnings("serial")
public class FlinkKafkaProducerITCase extends KafkaTestBase {
	protected String transactionalId;
	protected Properties extraProperties;

	@Before
	public void before() {
		transactionalId = UUID.randomUUID().toString();
		extraProperties = new Properties();
		extraProperties.putAll(standardProps);
		extraProperties.put("transactional.id", transactionalId);
		extraProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		extraProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		extraProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		extraProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		extraProperties.put("isolation.level", "read_committed");
	}

	@Test(timeout = 30000L)
	public void testHappyPath() throws IOException {
		String topicName = "flink-kafka-producer-happy-path";
		try (Producer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties)) {
			kafkaProducer.initTransactions();
			kafkaProducer.beginTransaction();
			kafkaProducer.send(new ProducerRecord<>(topicName, "42", "42"));
			kafkaProducer.commitTransaction();
		}
		assertRecord(topicName, "42", "42");
		deleteTestTopic(topicName);
	}

	@Test(timeout = 30000L)
	public void testResumeTransaction() throws IOException {
		String topicName = "flink-kafka-producer-resume-transaction";
		try (FlinkKafkaProducer<String, String> kafkaProducer = new FlinkKafkaProducer<>(extraProperties)) {
			kafkaProducer.initTransactions();
			kafkaProducer.beginTransaction();
			kafkaProducer.send(new ProducerRecord<>(topicName, "42", "42"));
			kafkaProducer.flush();
			long producerId = kafkaProducer.getProducerId();
			short epoch = kafkaProducer.getEpoch();

			try (FlinkKafkaProducer<String, String> resumeProducer = new FlinkKafkaProducer<>(extraProperties)) {
				resumeProducer.resumeTransaction(producerId, epoch);
				resumeProducer.commitTransaction();
			}

			assertRecord(topicName, "42", "42");

			// this shouldn't throw - in case of network split, old producer might attempt to commit it's transaction
			kafkaProducer.commitTransaction();

			// this shouldn't fail also, for same reason as above
			try (FlinkKafkaProducer<String, String> resumeProducer = new FlinkKafkaProducer<>(extraProperties)) {
				resumeProducer.resumeTransaction(producerId, epoch);
				resumeProducer.commitTransaction();
			}
		}
		deleteTestTopic(topicName);
	}

	private void assertRecord(String topicName, String expectedKey, String expectedValue) {
		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(extraProperties)) {
			kafkaConsumer.subscribe(Collections.singletonList(topicName));
			ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);

			ConsumerRecord<String, String> record = Iterables.getOnlyElement(records);
			assertEquals(expectedKey, record.key());
			assertEquals(expectedValue, record.value());
		}
	}
}

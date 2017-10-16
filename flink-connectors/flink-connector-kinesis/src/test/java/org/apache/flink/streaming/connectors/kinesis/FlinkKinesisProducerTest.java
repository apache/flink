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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Suite of {@link FlinkKinesisProducer} tests.
 */
public class FlinkKinesisProducerTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	// ----------------------------------------------------------------------
	// Tests to verify serializability
	// ----------------------------------------------------------------------

	@Test
	public void testCreateWithNonSerializableDeserializerFails() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("The provided serialization schema is not serializable");

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		new FlinkKinesisProducer<>(new NonSerializableSerializationSchema(), testConfig);
	}

	@Test
	public void testCreateWithSerializableDeserializer() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		new FlinkKinesisProducer<>(new SerializableSerializationSchema(), testConfig);
	}

	@Test
	public void testConfigureWithNonSerializableCustomPartitionerFails() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("The provided custom partitioner is not serializable");

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		new FlinkKinesisProducer<>(new SimpleStringSchema(), testConfig)
			.setCustomPartitioner(new NonSerializableCustomPartitioner());
	}

	@Test
	public void testConfigureWithSerializableCustomPartitioner() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		new FlinkKinesisProducer<>(new SimpleStringSchema(), testConfig)
			.setCustomPartitioner(new SerializableCustomPartitioner());
	}

	@Test
	public void testConsumerIsSerializable() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		FlinkKinesisProducer<String> consumer = new FlinkKinesisProducer<>(new SimpleStringSchema(), testConfig);
		assertTrue(InstantiationUtil.isSerializable(consumer));
	}

	// ----------------------------------------------------------------------
	// Utility test classes
	// ----------------------------------------------------------------------

	/**
	 * A non-serializable {@link KinesisSerializationSchema} (because it is a nested class with reference
	 * to the enclosing class, which is not serializable) used for testing.
	 */
	private final class NonSerializableSerializationSchema implements KinesisSerializationSchema<String> {
		@Override
		public ByteBuffer serialize(String element) {
			return ByteBuffer.wrap(element.getBytes());
		}

		@Override
		public String getTargetStream(String element) {
			return "test-stream";
		}
	}

	/**
	 * A static, serializable {@link KinesisSerializationSchema}.
	 */
	private static final class SerializableSerializationSchema implements KinesisSerializationSchema<String> {
		@Override
		public ByteBuffer serialize(String element) {
			return ByteBuffer.wrap(element.getBytes());
		}

		@Override
		public String getTargetStream(String element) {
			return "test-stream";
		}
	}

	/**
	 * A non-serializable {@link KinesisPartitioner} (because it is a nested class with reference
	 * to the enclosing class, which is not serializable) used for testing.
	 */
	private final class NonSerializableCustomPartitioner extends KinesisPartitioner<String> {
		@Override
		public String getPartitionId(String element) {
			return "test-partition";
		}
	}

	/**
	 * A static, serializable {@link KinesisPartitioner}.
	 */
	private static final class SerializableCustomPartitioner extends KinesisPartitioner<String> {
		@Override
		public String getPartitionId(String element) {
			return "test-partition";
		}
	}
}

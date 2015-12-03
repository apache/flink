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

import org.apache.commons.collections.map.LinkedMap;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KafkaConsumerTest {

	@Test
	public void testValidateZooKeeperConfig() {
		try {
			// empty
			Properties emptyProperties = new Properties();
			try {
				FlinkKafkaConsumer.validateZooKeeperConfig(emptyProperties);
				fail("should fail with an exception");
			}
			catch (IllegalArgumentException e) {
				// expected
			}

			// no connect string (only group string)
			Properties noConnect = new Properties();
			noConnect.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-test-group");
			try {
				FlinkKafkaConsumer.validateZooKeeperConfig(noConnect);
				fail("should fail with an exception");
			}
			catch (IllegalArgumentException e) {
				// expected
			}

			// no group string (only connect string)
			Properties noGroup = new Properties();
			noGroup.put("zookeeper.connect", "localhost:47574");
			try {
				FlinkKafkaConsumer.validateZooKeeperConfig(noGroup);
				fail("should fail with an exception");
			}
			catch (IllegalArgumentException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSnapshot() {
		try {
			Field offsetsField = FlinkKafkaConsumer.class.getDeclaredField("lastOffsets");
			Field runningField = FlinkKafkaConsumer.class.getDeclaredField("running");
			Field mapField = FlinkKafkaConsumer.class.getDeclaredField("pendingCheckpoints");

			offsetsField.setAccessible(true);
			runningField.setAccessible(true);
			mapField.setAccessible(true);

			FlinkKafkaConsumer<?> consumer = mock(FlinkKafkaConsumer.class);
			when(consumer.snapshotState(anyLong(), anyLong())).thenCallRealMethod();


			HashMap<KafkaTopicPartition, Long> testOffsets = new HashMap<>();
			long[] offsets = new long[] { 43, 6146, 133, 16, 162, 616 };
			int j = 0;
			for (long i: offsets) {
				KafkaTopicPartition ktp = new KafkaTopicPartition("topic", j++);
				testOffsets.put(ktp, i);
			}

			LinkedMap map = new LinkedMap();

			offsetsField.set(consumer, testOffsets);
			runningField.set(consumer, true);
			mapField.set(consumer, map);

			assertTrue(map.isEmpty());

			// make multiple checkpoints
			for (long checkpointId = 10L; checkpointId <= 2000L; checkpointId += 9L) {
				HashMap<KafkaTopicPartition, Long> checkpoint = consumer.snapshotState(checkpointId, 47 * checkpointId);
				assertEquals(testOffsets, checkpoint);

				// change the offsets, make sure the snapshot did not change
				HashMap<KafkaTopicPartition, Long> checkpointCopy = (HashMap<KafkaTopicPartition, Long>) checkpoint.clone();

				for (Map.Entry<KafkaTopicPartition, Long> e: testOffsets.entrySet()) {
					testOffsets.put(e.getKey(), e.getValue() + 1);
				}

				assertEquals(checkpointCopy, checkpoint);

				assertTrue(map.size() > 0);
				assertTrue(map.size() <= FlinkKafkaConsumer.MAX_NUM_PENDING_CHECKPOINTS);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	@Ignore("Kafka consumer internally makes an infinite loop")
	public void testCreateSourceWithoutCluster() {
		try {
			Properties props = new Properties();
			props.setProperty("zookeeper.connect", "localhost:56794");
			props.setProperty("bootstrap.servers", "localhost:11111, localhost:22222");
			props.setProperty("group.id", "non-existent-group");

			new FlinkKafkaConsumer<>(Collections.singletonList("no op topic"), new SimpleStringSchema(), props,
					FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
					FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

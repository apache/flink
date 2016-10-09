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

import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka09Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Unit tests for the {@link Kafka09Fetcher}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Kafka09Fetcher.class)
public class Kafka09FetcherTest {

	@Test
	public void testCommitDoesNotBlock() throws Exception {

		// test data
		final KafkaTopicPartition testPartition = new KafkaTopicPartition("test", 42);
		final Map<KafkaTopicPartition, Long> testCommitData = new HashMap<>();
		testCommitData.put(testPartition, 11L);

		// to synchronize when the consumer is in its blocking method
		final OneShotLatch sync = new OneShotLatch();

		// ----- the mock consumer with blocking poll calls ----
		final MultiShotLatch blockerLatch = new MultiShotLatch();
		
		KafkaConsumer<?, ?> mockConsumer = mock(KafkaConsumer.class);
		when(mockConsumer.poll(anyLong())).thenAnswer(new Answer<ConsumerRecords<?, ?>>() {
			
			@Override
			public ConsumerRecords<?, ?> answer(InvocationOnMock invocation) throws InterruptedException {
				sync.trigger();
				blockerLatch.await();
				return ConsumerRecords.empty();
			}
		});

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				blockerLatch.trigger();
				return null;
			}
		}).when(mockConsumer).wakeup();

		// make sure the fetcher creates the mock consumer
		whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

		// ----- create the test fetcher -----

		@SuppressWarnings("unchecked")
		SourceContext<String> sourceContext = mock(SourceContext.class);
		List<KafkaTopicPartition> topics = Collections.singletonList(new KafkaTopicPartition("test", 42));
		KeyedDeserializationSchema<String> schema = new KeyedDeserializationSchemaWrapper<>(new SimpleStringSchema());
		StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);
		
		final Kafka09Fetcher<String> fetcher = new Kafka09Fetcher<>(
				sourceContext, topics, null, null, context, schema, new Properties(), 0L, false);

		// ----- run the fetcher -----

		final AtomicReference<Throwable> error = new AtomicReference<>();
		final Thread fetcherRunner = new Thread("fetcher runner") {

			@Override
			public void run() {
				try {
					fetcher.runFetchLoop();
				} catch (Throwable t) {
					error.set(t);
				}
			}
		};
		fetcherRunner.start();

		// wait until the fetcher has reached the method of interest
		sync.await();

		// ----- trigger the offset commit -----
		
		final AtomicReference<Throwable> commitError = new AtomicReference<>();
		final Thread committer = new Thread("committer runner") {
			@Override
			public void run() {
				try {
					fetcher.commitSpecificOffsetsToKafka(testCommitData);
				} catch (Throwable t) {
					commitError.set(t);
				}
			}
		};
		committer.start();

		// ----- ensure that the committer finishes in time  -----
		committer.join(30000);
		assertFalse("The committer did not finish in time", committer.isAlive());

		// ----- test done, wait till the fetcher is done for a clean shutdown -----
		fetcher.cancel();
		fetcherRunner.join();

		// check that there were no errors in the fetcher
		final Throwable fetcherError = error.get();
		if (fetcherError != null) {
			throw new Exception("Exception in the fetcher", fetcherError);
		}
		final Throwable committerError = commitError.get();
		if (committerError != null) {
			throw new Exception("Exception in the committer", committerError);
		}
	}

	@Test
	public void ensureOffsetsGetCommitted() throws Exception {
		
		// test data
		final KafkaTopicPartition testPartition1 = new KafkaTopicPartition("test", 42);
		final KafkaTopicPartition testPartition2 = new KafkaTopicPartition("another", 99);
		
		final Map<KafkaTopicPartition, Long> testCommitData1 = new HashMap<>();
		testCommitData1.put(testPartition1, 11L);
		testCommitData1.put(testPartition2, 18L);

		final Map<KafkaTopicPartition, Long> testCommitData2 = new HashMap<>();
		testCommitData2.put(testPartition1, 19L);
		testCommitData2.put(testPartition2, 28L);

		final BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitStore = new LinkedBlockingQueue<>();


		// ----- the mock consumer with poll(), wakeup(), and commit(A)sync calls ----

		final MultiShotLatch blockerLatch = new MultiShotLatch();

		KafkaConsumer<?, ?> mockConsumer = mock(KafkaConsumer.class);

		when(mockConsumer.poll(anyLong())).thenAnswer(new Answer<ConsumerRecords<?, ?>>() {
			@Override
			public ConsumerRecords<?, ?> answer(InvocationOnMock invocation) throws InterruptedException {
				blockerLatch.await();
				return ConsumerRecords.empty();
			}
		});

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				blockerLatch.trigger();
				return null;
			}
		}).when(mockConsumer).wakeup();

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				@SuppressWarnings("unchecked")
				Map<TopicPartition, OffsetAndMetadata> offsets = 
						(Map<TopicPartition, OffsetAndMetadata>) invocation.getArguments()[0];

				OffsetCommitCallback callback = (OffsetCommitCallback) invocation.getArguments()[1];

				commitStore.add(offsets);
				callback.onComplete(offsets, null);

				return null; 
			}
		}).when(mockConsumer).commitAsync(
				Mockito.<Map<TopicPartition, OffsetAndMetadata>>any(), any(OffsetCommitCallback.class));

		// make sure the fetcher creates the mock consumer
		whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(mockConsumer);

		// ----- create the test fetcher -----

		@SuppressWarnings("unchecked")
		SourceContext<String> sourceContext = mock(SourceContext.class);
		List<KafkaTopicPartition> topics = Collections.singletonList(new KafkaTopicPartition("test", 42));
		KeyedDeserializationSchema<String> schema = new KeyedDeserializationSchemaWrapper<>(new SimpleStringSchema());
		StreamingRuntimeContext context = mock(StreamingRuntimeContext.class);

		final Kafka09Fetcher<String> fetcher = new Kafka09Fetcher<>(
				sourceContext, topics, null, null, context, schema, new Properties(), 0L, false);

		// ----- run the fetcher -----

		final AtomicReference<Throwable> error = new AtomicReference<>();
		final Thread fetcherRunner = new Thread("fetcher runner") {

			@Override
			public void run() {
				try {
					fetcher.runFetchLoop();
				} catch (Throwable t) {
					error.set(t);
				}
			}
		};
		fetcherRunner.start();

		// ----- trigger the first offset commit -----

		fetcher.commitSpecificOffsetsToKafka(testCommitData1);
		Map<TopicPartition, OffsetAndMetadata> result1 = commitStore.take();

		for (Entry<TopicPartition, OffsetAndMetadata> entry : result1.entrySet()) {
			TopicPartition partition = entry.getKey();
			if (partition.topic().equals("test")) {
				assertEquals(42, partition.partition());
				assertEquals(12L, entry.getValue().offset());
			}
			else if (partition.topic().equals("another")) {
				assertEquals(99, partition.partition());
				assertEquals(18L, entry.getValue().offset());
			}
		}

		// ----- trigger the second offset commit -----

		fetcher.commitSpecificOffsetsToKafka(testCommitData2);
		Map<TopicPartition, OffsetAndMetadata> result2 = commitStore.take();

		for (Entry<TopicPartition, OffsetAndMetadata> entry : result2.entrySet()) {
			TopicPartition partition = entry.getKey();
			if (partition.topic().equals("test")) {
				assertEquals(42, partition.partition());
				assertEquals(20L, entry.getValue().offset());
			}
			else if (partition.topic().equals("another")) {
				assertEquals(99, partition.partition());
				assertEquals(28L, entry.getValue().offset());
			}
		}
		
		// ----- test done, wait till the fetcher is done for a clean shutdown -----
		fetcher.cancel();
		fetcherRunner.join();

		// check that there were no errors in the fetcher
		final Throwable caughtError = error.get();
		if (caughtError != null) {
			throw new Exception("Exception in the fetcher", caughtError);
		}
	}
}

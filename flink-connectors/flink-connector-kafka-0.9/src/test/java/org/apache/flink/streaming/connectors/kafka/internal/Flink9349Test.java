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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaCommitCallback;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.anyLong;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Unit tests for the {@link Flink9349Test}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaConsumerThread.class)
public class Flink9349Test {
	@Test
	public void testConcurrentPartitionsDiscoveryAndLoopFetching() throws Exception {

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
		SourceFunction.SourceContext<String> sourceContext = mock(SourceFunction.SourceContext.class);
		Map<KafkaTopicPartition, Long> partitionsWithInitialOffsets =
			Collections.singletonMap(new KafkaTopicPartition("test", 42), KafkaTopicPartitionStateSentinel.GROUP_OFFSET);
		KeyedDeserializationSchema<String> schema = new KeyedDeserializationSchemaWrapper<>(new SimpleStringSchema());

		final Kafka09Fetcher<String> fetcher = new Kafka09Fetcher<>(
			sourceContext,
			partitionsWithInitialOffsets,
			null, /* periodic watermark extractor */
			null, /* punctuated watermark extractor */
			new TestProcessingTimeService(),
			10, /* watermark interval */
			this.getClass().getClassLoader(),
			"task_name",
			schema,
			new Properties(),
			0L,
			new UnregisteredMetricsGroup(),
			new UnregisteredMetricsGroup(),
			false);

		// ----- run the fetcher -----

		final AtomicReference<Throwable> error = new AtomicReference<>();
		int fetchTasks = 2;
		final CountDownLatch latch = new CountDownLatch(fetchTasks);
		ExecutorService service = Executors.newFixedThreadPool(fetchTasks + 1);

		service.submit(new Thread("fetcher runner ") {
			@Override
			public void run() {
				try {
					latch.await();
					fetcher.runFetchLoop();
				} catch (Throwable t) {
					error.set(t);
				}
			}
		});
		for (int i = 0; i < fetchTasks; i++) {
			service.submit(new Thread("add partitions " + i) {

				@Override
				public void run() {
					try {
						List<KafkaTopicPartition> newPartitions = new ArrayList<>();
						for (int i = 0; i < 1000; i++) {
							newPartitions.add(testPartition);
						}
						fetcher.addDiscoveredPartitions(newPartitions);
						latch.countDown();
						//latch.await();
						for (int i = 0; i < 100; i++) {
							fetcher.addDiscoveredPartitions(newPartitions);
							Thread.sleep(1L);
						}
					} catch (Throwable t) {
						error.set(t);
					}
				}
			});
		}

		service.awaitTermination(1L, TimeUnit.SECONDS);

		// wait until the fetcher has reached the method of interest
		sync.await();

		// ----- trigger the offset commit -----

		final AtomicReference<Throwable> commitError = new AtomicReference<>();
		final Thread committer = new Thread("committer runner") {
			@Override
			public void run() {
				try {
					fetcher.commitInternalOffsetsToKafka(testCommitData, mock(KafkaCommitCallback.class));
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

		// check that there were no errors in the fetcher
		final Throwable fetcherError = error.get();
		if (fetcherError != null && !(fetcherError instanceof Handover.ClosedException)) {
			throw new Exception("Exception in the fetcher", fetcherError);
		}
		final Throwable committerError = commitError.get();
		if (committerError != null) {
			throw new Exception("Exception in the committer", committerError);
		}

	}
}

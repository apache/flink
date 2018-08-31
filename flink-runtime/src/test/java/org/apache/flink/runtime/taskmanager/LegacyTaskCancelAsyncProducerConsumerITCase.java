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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobStatus;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.io.network.buffer.LocalBufferPoolDestroyTest.isInBlockingBufferRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LegacyTaskCancelAsyncProducerConsumerITCase extends TestLogger {

	// The Exceptions thrown by the producer/consumer Threads
	private static volatile Exception ASYNC_PRODUCER_EXCEPTION;
	private static volatile Exception ASYNC_CONSUMER_EXCEPTION;

	// The Threads producing/consuming the intermediate stream
	private static volatile Thread ASYNC_PRODUCER_THREAD;
	private static volatile Thread ASYNC_CONSUMER_THREAD;

	/**
	 * Tests that a task waiting on an async producer/consumer that is stuck
	 * in a blocking buffer request can be properly cancelled.
	 *
	 * <p>This is currently required for the Flink Kafka sources, which spawn
	 * a separate Thread consuming from Kafka and producing the intermediate
	 * streams in the spawned Thread instead of the main task Thread.
	 */
	@Test
	public void testCancelAsyncProducerAndConsumer() throws Exception {
		Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();
		TestingCluster flink = null;

		try {
			// Cluster
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
			config.setString(TaskManagerOptions.MEMORY_SEGMENT_SIZE, "4096");
			config.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 9);

			flink = new TestingCluster(config, true);
			flink.start();

			// Job with async producer and consumer
			JobVertex producer = new JobVertex("AsyncProducer");
			producer.setParallelism(1);
			producer.setInvokableClass(AsyncProducer.class);

			JobVertex consumer = new JobVertex("AsyncConsumer");
			consumer.setParallelism(1);
			consumer.setInvokableClass(AsyncConsumer.class);
			consumer.connectNewDataSetAsInput(producer, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			SlotSharingGroup slot = new SlotSharingGroup(producer.getID(), consumer.getID());
			producer.setSlotSharingGroup(slot);
			consumer.setSlotSharingGroup(slot);

			JobGraph jobGraph = new JobGraph(producer, consumer);

			// Submit job and wait until running
			ActorGateway jobManager = flink.getLeaderGateway(deadline.timeLeft());
			flink.submitJobDetached(jobGraph);

			Object msg = new WaitForAllVerticesToBeRunning(jobGraph.getJobID());
			Future<?> runningFuture = jobManager.ask(msg, deadline.timeLeft());
			Await.ready(runningFuture, deadline.timeLeft());

			// Wait for blocking requests, cancel and wait for cancellation
			msg = new NotifyWhenJobStatus(jobGraph.getJobID(), JobStatus.CANCELED);
			Future<?> cancelledFuture = jobManager.ask(msg, deadline.timeLeft());

			boolean producerBlocked = false;
			for (int i = 0; i < 50; i++) {
				Thread thread = ASYNC_PRODUCER_THREAD;

				if (thread != null && thread.isAlive()) {
					StackTraceElement[] stackTrace = thread.getStackTrace();
					producerBlocked = isInBlockingBufferRequest(stackTrace);
				}

				if (producerBlocked) {
					break;
				} else {
					// Retry
					Thread.sleep(500L);
				}
			}

			// Verify that async producer is in blocking request
			assertTrue("Producer thread is not blocked: " + Arrays.toString(ASYNC_PRODUCER_THREAD.getStackTrace()), producerBlocked);

			boolean consumerWaiting = false;
			for (int i = 0; i < 50; i++) {
				Thread thread = ASYNC_CONSUMER_THREAD;

				if (thread != null && thread.isAlive()) {
					consumerWaiting = thread.getState() == Thread.State.WAITING;
				}

				if (consumerWaiting) {
					break;
				} else {
					// Retry
					Thread.sleep(500L);
				}
			}

			// Verify that async consumer is in blocking request
			assertTrue("Consumer thread is not blocked.", consumerWaiting);

			msg = new CancelJob(jobGraph.getJobID());
			Future<?> cancelFuture = jobManager.ask(msg, deadline.timeLeft());
			Await.ready(cancelFuture, deadline.timeLeft());

			Await.ready(cancelledFuture, deadline.timeLeft());

			// Verify the expected Exceptions
			assertNotNull(ASYNC_PRODUCER_EXCEPTION);
			assertEquals(IllegalStateException.class, ASYNC_PRODUCER_EXCEPTION.getClass());

			assertNotNull(ASYNC_CONSUMER_EXCEPTION);
			assertEquals(IllegalStateException.class, ASYNC_CONSUMER_EXCEPTION.getClass());
		} finally {
			if (flink != null) {
				flink.stop();
			}
		}
	}

	/**
	 * Invokable emitting records in a separate Thread (not the main Task
	 * thread).
	 */
	public static class AsyncProducer extends AbstractInvokable {

		public AsyncProducer(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			Thread producer = new ProducerThread(getEnvironment().getWriter(0));

			// Publish the async producer for the main test Thread
			ASYNC_PRODUCER_THREAD = producer;

			producer.start();

			// Wait for the producer Thread to finish. This is executed in the
			// main Task thread and will be interrupted on cancellation.
			while (producer.isAlive()) {
				try {
					producer.join();
				} catch (InterruptedException ignored) {
				}
			}
		}

		/**
		 * The Thread emitting the records.
		 */
		private static class ProducerThread extends Thread {

			private final RecordWriter<LongValue> recordWriter;

			public ProducerThread(ResultPartitionWriter partitionWriter) {
				this.recordWriter = new RecordWriter<>(partitionWriter);
			}

			@Override
			public void run() {
				LongValue current = new LongValue(0);

				try {
					while (true) {
						current.setValue(current.getValue() + 1);
						recordWriter.emit(current);
						recordWriter.flushAll();
					}
				} catch (Exception e) {
					ASYNC_PRODUCER_EXCEPTION = e;
				}
			}
		}
	}

	/**
	 * Invokable consuming buffers in a separate Thread (not the main Task
	 * thread).
	 */
	public static class AsyncConsumer extends AbstractInvokable {

		public AsyncConsumer(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			Thread consumer = new ConsumerThread(getEnvironment().getInputGate(0));

			// Publish the async consumer for the main test Thread
			ASYNC_CONSUMER_THREAD = consumer;

			consumer.start();

			// Wait for the consumer Thread to finish. This is executed in the
			// main Task thread and will be interrupted on cancellation.
			while (consumer.isAlive()) {
				try {
					consumer.join();
				} catch (InterruptedException ignored) {
				}
			}
		}

		/**
		 * The Thread consuming buffers.
		 */
		private static class ConsumerThread extends Thread {

			private final InputGate inputGate;

			public ConsumerThread(InputGate inputGate) {
				this.inputGate = inputGate;
			}

			@Override
			public void run() {
				try {
					while (true) {
						inputGate.getNextBufferOrEvent();
					}
				} catch (Exception e) {
					ASYNC_CONSUMER_EXCEPTION = e;
				}
			}
		}
	}
}

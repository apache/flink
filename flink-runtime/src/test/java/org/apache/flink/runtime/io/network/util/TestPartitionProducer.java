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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.util.TestProducerSource.BufferConsumerAndChannel;

import java.util.Random;
import java.util.concurrent.Callable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A test partition producer.
 *
 * <p> The behaviour of the producer is customizable by specifying a source.
 *
 * @see TestProducerSource
 */
public class TestPartitionProducer implements Callable<Boolean> {

	public static final int MAX_SLEEP_TIME_MS = 20;

	/** The partition to add data to. */
	private final ResultPartition partition;

	/**
	 * Flag indicating whether the consumer is slow. If true, the consumer will sleep a random
	 * number of milliseconds between adding data.
	 */
	private final boolean isSlowProducer;

	/** The source data. */
	private final TestProducerSource source;

	/** Random source for sleeps. */
	private final Random random;

	public TestPartitionProducer(
			ResultPartition partition,
			boolean isSlowProducer,
			TestProducerSource source) {

		this.partition = checkNotNull(partition);
		this.isSlowProducer = isSlowProducer;
		this.random = isSlowProducer ? new Random() : null;
		this.source = checkNotNull(source);
	}

	@Override
	public Boolean call() throws Exception {

		boolean success = false;

		try {
			BufferConsumerAndChannel consumerAndChannel;

			while ((consumerAndChannel = source.getNextBufferConsumer()) != null) {
				partition.addBufferConsumer(consumerAndChannel.getBufferConsumer(), consumerAndChannel.getTargetChannel());

				// Check for interrupted flag after adding data to prevent resource leaks
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}

				if (isSlowProducer) {
					Thread.sleep(random.nextInt(MAX_SLEEP_TIME_MS + 1));
				}
			}

			partition.finish();

			success = true;

			return true;
		}
		finally {
			if (!success) {
				partition.release();
			}
		}
	}
}

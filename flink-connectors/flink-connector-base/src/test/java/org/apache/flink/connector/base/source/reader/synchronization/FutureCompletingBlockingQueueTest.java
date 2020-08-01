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

package org.apache.flink.connector.base.source.reader.synchronization;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * The unit test for {@link FutureCompletingBlockingQueue}.
 */
public class FutureCompletingBlockingQueueTest {


	private static final Integer DEFAULT_CAPACITY = 10000;
	private static final Integer SPECIFIED_CAPACITY = 20000;

	@Test
	public void testFutureCompletingBlockingQueueConstructor() {
		FutureNotifier notifier = new FutureNotifier();
		FutureCompletingBlockingQueue<Object> defaultCapacityFutureCompletingBlockingQueue = new FutureCompletingBlockingQueue<>(notifier);
		FutureCompletingBlockingQueue<Object> specifiedCapacityFutureCompletingBlockingQueue = new FutureCompletingBlockingQueue<>(notifier, SPECIFIED_CAPACITY);
		// The capacity of the queue needs to be equal to 10000
		assertEquals(defaultCapacityFutureCompletingBlockingQueue.remainingCapacity(), (int) DEFAULT_CAPACITY);
		// The capacity of the queue needs to be equal to SPECIFIED_CAPACITY
		assertEquals(specifiedCapacityFutureCompletingBlockingQueue.remainingCapacity(), (int) SPECIFIED_CAPACITY);
	}
}

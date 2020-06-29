package org.apache.flink.connector.base.source.reader.synchronization;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * The unit test for {@link FutureCompletingBlockingQueue}
 */
public class FutureCompletingBlockingQueueTest {

	private static final Integer DEFAULT_CAPACITY = 10000;

	@Test
	public void testFutureCompletingBlockingQueueConstructor(){
		FutureNotifier notifier = new FutureNotifier();
		FutureCompletingBlockingQueue<Object> defaultCapacityFutureCompletingBlockingQueue = new FutureCompletingBlockingQueue<>(notifier);
		FutureCompletingBlockingQueue<Object> specifiedCapacityFutureCompletingBlockingQueue = new FutureCompletingBlockingQueue<>(notifier, DEFAULT_CAPACITY);
		assertEquals(defaultCapacityFutureCompletingBlockingQueue.remainingCapacity(),(int)DEFAULT_CAPACITY);
		assertEquals(specifiedCapacityFutureCompletingBlockingQueue.remainingCapacity(),(int)DEFAULT_CAPACITY);
	}
}

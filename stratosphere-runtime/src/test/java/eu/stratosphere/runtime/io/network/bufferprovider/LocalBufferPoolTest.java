/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.bufferprovider;

import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider.BufferAvailabilityRegistration;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LocalBufferPoolTest {

	private final static int NUM_BUFFERS = 2048;

	private final static int BUFFER_SIZE = 1024;

	private final static GlobalBufferPool GLOBAL_BUFFER_POOL = new GlobalBufferPool(NUM_BUFFERS, BUFFER_SIZE);

	private final static RecyclingBufferAvailableAnswer RECYCLING_BUFFER_AVAILABLE_ANSWER = new RecyclingBufferAvailableAnswer();

	@BeforeClass
	public static void setupGlobalBufferPoolOnce() {
		Assert.assertEquals("GlobalBufferPool does not have required number of buffers.",
				NUM_BUFFERS, GLOBAL_BUFFER_POOL.numBuffers());
		Assert.assertEquals("GlobalBufferPool does not have required number of available buffers.",
				NUM_BUFFERS, GLOBAL_BUFFER_POOL.numAvailableBuffers());
	}

	@After
	public void verifyAllBuffersReturnedToGlobalBufferPool() {
		Assert.assertEquals("Did not return all buffers to GlobalBufferPool after test.",
				NUM_BUFFERS, GLOBAL_BUFFER_POOL.numAvailableBuffers());
	}

	@Test
	public void testSingleConsumerNonBlockingRequestAndRecycle() throws IOException {
		final LocalBufferPool bufferPool = new LocalBufferPool(GLOBAL_BUFFER_POOL, NUM_BUFFERS);

		Assert.assertEquals(0, bufferPool.numRequestedBuffers());

		// this request-recycle cycle should only take a single buffer out of
		// the GlobalBufferPool as it is recycled over and over again
		for (int numRequested = 0; numRequested < NUM_BUFFERS; numRequested++) {
			Buffer buffer = bufferPool.requestBuffer(BUFFER_SIZE);

			Assert.assertEquals(BUFFER_SIZE, buffer.size());

			Assert.assertEquals("Expected single buffer request in buffer pool.",
					1, bufferPool.numRequestedBuffers());
			Assert.assertEquals("Expected no available buffer in buffer pool.",
					0, bufferPool.numAvailableBuffers());

			buffer.recycleBuffer();

			Assert.assertEquals("Expected single available buffer after recycle.",
					1, bufferPool.numAvailableBuffers());
		}

		bufferPool.destroy();
	}

	@Test
	public void testSingleConsumerNonBlockingRequestMoreThanAvailable() throws IOException {
		final LocalBufferPool bufferPool = new LocalBufferPool(GLOBAL_BUFFER_POOL, NUM_BUFFERS);

		Assert.assertEquals(0, bufferPool.numRequestedBuffers());

		// request all buffers from the buffer pool
		Buffer[] requestedBuffers = new Buffer[NUM_BUFFERS];
		for (int i = 0; i < NUM_BUFFERS; i++) {
			requestedBuffers[i] = bufferPool.requestBuffer(BUFFER_SIZE);
		}

		Assert.assertEquals("Expected no available buffer in buffer pool.",
				0, bufferPool.numAvailableBuffers());

		Assert.assertNull("Expected null return value for buffer request with no available buffer.",
				bufferPool.requestBuffer(BUFFER_SIZE));

		// recycle all buffers and destroy buffer pool
		for (Buffer buffer : requestedBuffers) {
			buffer.recycleBuffer();
		}

		bufferPool.destroy();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSingleConsumerNonBlockingRequestTooLarge() throws IOException {
		final LocalBufferPool bufferPool = new LocalBufferPool(GLOBAL_BUFFER_POOL, NUM_BUFFERS);

		// request too large buffer for the pool
		bufferPool.requestBuffer(BUFFER_SIZE * 2);
	}

	@Test
	public void testSingleConsumerNonBlockingRequestSmall() throws IOException {
		final LocalBufferPool bufferPool = new LocalBufferPool(GLOBAL_BUFFER_POOL, NUM_BUFFERS);

		// request smaller buffer and verify size
		Buffer buffer = bufferPool.requestBuffer(BUFFER_SIZE / 2);

		Assert.assertEquals(BUFFER_SIZE / 2, buffer.size());

		buffer.recycleBuffer();

		bufferPool.destroy();
	}

	@Test
	public void testSingleConsumerBlockingRequest() throws Exception {
		final LocalBufferPool bufferPool = new LocalBufferPool(GLOBAL_BUFFER_POOL, NUM_BUFFERS);

		final Buffer[] requestedBuffers = new Buffer[NUM_BUFFERS];
		for (int i = 0; i < NUM_BUFFERS; i++) {
			requestedBuffers[i] = bufferPool.requestBuffer(BUFFER_SIZE);
		}

		final Buffer[] bufferFromBlockingRequest = new Buffer[1];

		// --------------------------------------------------------------------
		// 1. blocking call: interrupt thread
		// --------------------------------------------------------------------
		Assert.assertEquals(NUM_BUFFERS, bufferPool.numRequestedBuffers());
		Assert.assertEquals(0, bufferPool.numAvailableBuffers());

		Thread blockingBufferRequestThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					bufferFromBlockingRequest[0] = bufferPool.requestBufferBlocking(BUFFER_SIZE);
					Assert.fail("Unexpected return from blocking buffer request.");
				} catch (IOException e) {
					Assert.fail("Unexpected IOException during test.");
				} catch (InterruptedException e) {
					// expected interruption
				}
			}
		});

		// start blocking request thread, sleep, interrupt blocking request thread
		blockingBufferRequestThread.start();

		Thread.sleep(500);

		blockingBufferRequestThread.interrupt();

		Assert.assertNull(bufferFromBlockingRequest[0]);
		Assert.assertEquals(NUM_BUFFERS, bufferPool.numRequestedBuffers());
		Assert.assertEquals(0, bufferPool.numAvailableBuffers());

		// --------------------------------------------------------------------
		// 2. blocking call: recycle buffer in different thread
		// --------------------------------------------------------------------
		// recycle the buffer soon
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				requestedBuffers[0].recycleBuffer();
			}
		}, 500);

		//
		try {
			Buffer buffer = bufferPool.requestBufferBlocking(BUFFER_SIZE);
			Assert.assertNotNull(buffer);

			buffer.recycleBuffer();
		} catch (InterruptedException e) {
			Assert.fail("Unexpected InterruptedException during test.");
		}

		// recycle remaining buffers
		for (int i = 1; i < requestedBuffers.length; i++) {
			requestedBuffers[i].recycleBuffer();
		}

		bufferPool.destroy();
	}

	@Test
	public void testSingleConsumerRecycleAfterDestroy() throws IOException {
		final LocalBufferPool bufferPool = new LocalBufferPool(GLOBAL_BUFFER_POOL, NUM_BUFFERS);

		Buffer[] requestedBuffers = new Buffer[NUM_BUFFERS];
		for (int i = 0; i < NUM_BUFFERS; i++) {
			requestedBuffers[i] = bufferPool.requestBuffer(BUFFER_SIZE);
		}

		bufferPool.destroy();

		// recycle should return buffers to GlobalBufferPool
		// => verified in verifyAllBuffersReturned()
		for (Buffer buffer : requestedBuffers) {
			buffer.recycleBuffer();
		}
	}

	@Test
	public void testSingleConsumerBufferAvailabilityListenerRegistration() throws Exception {
		final LocalBufferPool bufferPool = new LocalBufferPool(GLOBAL_BUFFER_POOL, NUM_BUFFERS);

		BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);

		// recycle buffer when listener mock is called back
		doAnswer(RECYCLING_BUFFER_AVAILABLE_ANSWER).when(listener).bufferAvailable(Matchers.<Buffer>anyObject());

		// request all buffers of the pool
		Buffer[] requestedBuffers = new Buffer[NUM_BUFFERS];
		for (int i = 0; i < NUM_BUFFERS; i++) {
			requestedBuffers[i] = bufferPool.requestBuffer(BUFFER_SIZE);
		}

		BufferAvailabilityRegistration registration;
		// --------------------------------------------------------------------
		// 1. success
		// --------------------------------------------------------------------
		registration = bufferPool.registerBufferAvailabilityListener(listener);
		Assert.assertEquals(BufferAvailabilityRegistration.SUCCEEDED_REGISTERED, registration);

		// verify call to buffer listener after recycle
		requestedBuffers[0].recycleBuffer();
		verify(listener, times(1)).bufferAvailable(Matchers.<Buffer>anyObject());

		Assert.assertEquals("Expected single available buffer after recycle call in mock listener.",
				1, bufferPool.numAvailableBuffers());

		// --------------------------------------------------------------------
		// 2. failure: buffer is available
		// --------------------------------------------------------------------
		registration = bufferPool.registerBufferAvailabilityListener(listener);
		Assert.assertEquals(BufferAvailabilityRegistration.FAILED_BUFFER_AVAILABLE, registration);

		Buffer buffer = bufferPool.requestBuffer(BUFFER_SIZE);
		Assert.assertNotNull(buffer);

		buffer.recycleBuffer();

		// --------------------------------------------------------------------
		// 3. failure: buffer pool destroyed
		// --------------------------------------------------------------------
		bufferPool.destroy();

		registration = bufferPool.registerBufferAvailabilityListener(listener);
		Assert.assertEquals(BufferAvailabilityRegistration.FAILED_BUFFER_POOL_DESTROYED, registration);

		// recycle remaining buffers
		for (int i = 1; i < requestedBuffers.length; i++) {
			requestedBuffers[i].recycleBuffer();
		}
	}

	@Test
	public void testSingleConsumerReturnExcessBuffers() throws Exception {
		final LocalBufferPool bufferPool = new LocalBufferPool(GLOBAL_BUFFER_POOL, NUM_BUFFERS);

		// request all buffers of the pool
		Buffer[] requestedBuffers = new Buffer[NUM_BUFFERS];
		for (int i = 0; i < NUM_BUFFERS; i++) {
			requestedBuffers[i] = bufferPool.requestBuffer(BUFFER_SIZE);
		}

		Assert.assertEquals(NUM_BUFFERS, bufferPool.numRequestedBuffers());
		Assert.assertEquals(0, bufferPool.numAvailableBuffers());

		// recycle first half of the buffers
		// => leave requested number of buffers unchanged
		// => increase available number of buffers
		for (int i = 0; i < NUM_BUFFERS / 2; i++) {
			requestedBuffers[i].recycleBuffer();
		}

		Assert.assertEquals(NUM_BUFFERS, bufferPool.numRequestedBuffers());
		Assert.assertEquals(NUM_BUFFERS / 2, bufferPool.numAvailableBuffers());

		// reduce designated number of buffers
		// => available buffers (1/2th) should be returned immediately
		// => non-available buffers (1/4th) should be returned later
		bufferPool.setNumDesignatedBuffers((NUM_BUFFERS / 2) - (NUM_BUFFERS / 4));

		Assert.assertEquals(NUM_BUFFERS / 2, bufferPool.numRequestedBuffers());
		Assert.assertEquals(0, bufferPool.numAvailableBuffers());

		// recycle second half of the buffers
		// => previously non-available buffers (1/4th) should be returned immediately
		// => remaining buffers are the available ones (1/4th)
		for (int i = NUM_BUFFERS / 2; i < NUM_BUFFERS; i++) {
			requestedBuffers[i].recycleBuffer();
		}

		Assert.assertEquals("Expected current number of requested buffers to be equal to the number of designated buffers.",
				bufferPool.numDesignatedBuffers(), bufferPool.numRequestedBuffers());

		Assert.assertEquals("Expected current number of requested and available buffers to be equal, " +
				"because all requested buffers have been recycled and become available again.",
				bufferPool.numRequestedBuffers(), bufferPool.numAvailableBuffers());

		// re-request remaining buffers and register buffer availability listener
		int remaining = bufferPool.numRequestedBuffers();
		for (int i = 0; i < remaining; i++) {
			requestedBuffers[i] = bufferPool.requestBuffer(BUFFER_SIZE);
		}

		BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);
		doAnswer(RECYCLING_BUFFER_AVAILABLE_ANSWER).when(listener).bufferAvailable(Matchers.<Buffer>anyObject());

		BufferAvailabilityRegistration registration = bufferPool.registerBufferAvailabilityListener(listener);
		Assert.assertEquals(BufferAvailabilityRegistration.SUCCEEDED_REGISTERED, registration);

		// reduce number of designated buffers and recycle all buffers
		bufferPool.setNumDesignatedBuffers(bufferPool.numDesignatedBuffers() - 1);

		for (int i = 0; i < remaining; i++) {
			requestedBuffers[i].recycleBuffer();
		}

		Assert.assertEquals(remaining - 1, bufferPool.numRequestedBuffers());
		Assert.assertEquals(remaining - 1, bufferPool.numAvailableBuffers());

		bufferPool.destroy();
	}

	// --------------------------------------------------------------------

	private static class RecyclingBufferAvailableAnswer implements Answer<Void> {

		@Override
		public Void answer(InvocationOnMock invocation) throws Throwable {
			Buffer buffer = (Buffer) invocation.getArguments()[0];
			buffer.recycleBuffer();

			return null;
		}
	}

}

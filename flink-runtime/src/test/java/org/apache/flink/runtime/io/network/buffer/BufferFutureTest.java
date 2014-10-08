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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferFutureTest {

	private final static int BUFFER_SIZE = 1024;

	private final static Buffer BUFFER = Mockito.mock(Buffer.class);

	static {
		Mockito.when(BUFFER.getSize()).thenReturn(BUFFER_SIZE);
	}

	@Test
	public void testFutureWithBuffer() {
		BufferFuture future = new BufferFuture(BUFFER);
		Assert.assertEquals(BUFFER, future.getBuffer());
		Assert.assertEquals(BUFFER_SIZE, future.getBufferSize());
	}

	@Test
	public void testBufferHandIn() {
		BufferFuture future = new BufferFuture(BUFFER_SIZE);
		Assert.assertNull(future.getBuffer());
		Assert.assertEquals(BUFFER_SIZE, future.getBufferSize());

		Buffer buffer = Mockito.mock(Buffer.class);
		future.handInBuffer(buffer);

		Assert.assertEquals(buffer, future.getBuffer());
	}

	@Test
	public void testBufferSizeAfterHandIn() {
		BufferFuture future = new BufferFuture(BUFFER_SIZE);
		Assert.assertNull(future.getBuffer());
		Assert.assertEquals(BUFFER_SIZE, future.getBufferSize());

		MemorySegment segment = new MemorySegment(new byte[BUFFER_SIZE * 2]);
		Buffer buffer = new Buffer(segment, BUFFER_SIZE * 2, Mockito.mock(BufferRecycler.class));

		// Hand in too large buffer
		future.handInBuffer(buffer);

		Assert.assertEquals(buffer, future.getBuffer());
		Assert.assertEquals(BUFFER_SIZE, future.getBuffer().getSize());
	}

	@Test
	public void testMultipleBufferHandInException() {
		BufferFuture future = new BufferFuture(BUFFER);

		Assert.assertEquals(BUFFER, future.getBuffer());

		try {
			future.handInBuffer(BUFFER);
			Assert.fail("Did not throw expected exception after second buffer hand in.");
		} catch (IllegalStateException e) {
			// OK => expected exceptions
		}
	}

	@Test
	public void testBufferHandInWithListener() {
		final AtomicBoolean hasCalledListener = new AtomicBoolean(false);

		BufferFuture future = new BufferFuture(BUFFER_SIZE).addListener(
				new EventListener<BufferFuture>() {
					@Override
					public void onEvent(BufferFuture bufferFuture) {
						Assert.assertEquals(BUFFER, bufferFuture.getBuffer());
						hasCalledListener.compareAndSet(false, true);
					}
				});

		future.handInBuffer(BUFFER);

		Assert.assertTrue(hasCalledListener.get());
	}

	@Test
	public void testLateListener() {
		final AtomicBoolean hasCalledListener = new AtomicBoolean(false);

		new BufferFuture(BUFFER).addListener(
				new EventListener<BufferFuture>() {
					@Override
					public void onEvent(BufferFuture bufferFuture) {
						Assert.assertEquals(BUFFER, bufferFuture.getBuffer());
						hasCalledListener.compareAndSet(false, true);
					}
				});

		Assert.assertTrue(hasCalledListener.get());
	}

	@Test
	public void testWaitForAvailableBuffer() {
		BufferFuture future = new BufferFuture(BUFFER);

		try {
			Assert.assertEquals(BUFFER, future.waitForBuffer().getBuffer());
		} catch (InterruptedException e) {
			Assert.fail("Unexpected exception while waiting for buffer hand in.");
		}
	}

	@Test
	public void testWaitForBufferHandIn() {
		final AtomicBoolean hasCalledListener = new AtomicBoolean(false);

		BufferFuture future = new BufferFuture(BUFFER_SIZE).addListener(
				new EventListener<BufferFuture>() {
					@Override
					public void onEvent(BufferFuture bufferFuture) {
						Assert.assertEquals(BUFFER, bufferFuture.getBuffer());
						hasCalledListener.compareAndSet(false, true);
					}
				});

		new Timer().schedule(new BufferDeliveryBoy(future, BUFFER), 250);

		try {
			Assert.assertEquals(BUFFER, future.waitForBuffer().getBuffer());
		} catch (InterruptedException e) {
			Assert.fail("Unexpected exception while waiting for buffer hand in.");
		}

		Assert.assertTrue(hasCalledListener.get());
	}

	@Test
	public void testWaitForBufferHandInAndListener() {
		BufferFuture future = new BufferFuture(BUFFER_SIZE);

		new Timer().schedule(new BufferDeliveryBoy(future, BUFFER), 250);

		try {
			Assert.assertEquals(BUFFER, future.waitForBuffer().getBuffer());
		} catch (InterruptedException e) {
			Assert.fail("Unexpected exception while waiting for buffer hand in.");
		}
	}

	@Test
	public void testHandInAfterCancel() {
		BufferFuture future = new BufferFuture(BUFFER_SIZE);

		future.cancel();

		future.handInBuffer(BUFFER);

		Mockito.verify(BUFFER, Mockito.times(1)).recycle();
	}

	@Test
	public void testWaitForBufferAfterCancel() {
		BufferFuture future = new BufferFuture(BUFFER_SIZE);

		future.cancel();

		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isCancelled());
		Assert.assertFalse(future.isSuccess());

		try {
			Assert.assertNull(future.waitForBuffer().getBuffer());
		} catch (InterruptedException e) {
			Assert.fail("Unexpected exception while waiting for buffer hand in.");
		}
	}

	@Test
	public void testCancelWhileWaitingForBuffer() {
		final BufferFuture future = new BufferFuture(BUFFER_SIZE);

		// Cancelling timer task
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				future.cancel();
			}
		}, 250);

		try {
			Assert.assertNull(future.waitForBuffer().getBuffer());

			Assert.assertTrue(future.isDone());
			Assert.assertTrue(future.isCancelled());
			Assert.assertFalse(future.isSuccess());
		} catch (InterruptedException e) {
			Assert.fail("Unexpected exception while waiting for buffer hand in.");
		}
	}

	@Test
	public void testCancelWithListener() {
		EventListener<BufferFuture> listener = Mockito.mock(EventListener.class);

		BufferFuture future = new BufferFuture(BUFFER_SIZE).addListener(listener);

		future.cancel();

		Mockito.verify(listener, Mockito.times(1)).onEvent(Matchers.any(BufferFuture.class));

		Assert.assertTrue(future.isDone());
		Assert.assertTrue(future.isCancelled());
		Assert.assertFalse(future.isSuccess());
	}

	@Test
	public void testCancelAfterBufferHandIn() {
		final BufferFuture future = new BufferFuture(BUFFER_SIZE);

		future.handInBuffer(BUFFER);

		try {
			future.cancel();
			Assert.fail("Did not throw expected exception.");
		} catch (IllegalStateException e) {
			// OK => expected exception
		}
	}

	// ------------------------------------------------------------------------

	private static class BufferDeliveryBoy extends TimerTask {

		private final BufferFuture future;

		private final Buffer buffer;

		private BufferDeliveryBoy(BufferFuture future, Buffer buffer) {
			this.future = future;
			this.buffer = buffer;
		}

		@Override
		public void run() {
			future.handInBuffer(buffer);
		}
	}
}

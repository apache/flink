/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestDispatcher.NO_OP;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link ChannelStateWriteRequestExecutorImpl} test.
 */
public class ChannelStateWriteRequestExecutorImplTest {

	private static final String TASK_NAME = "test task";

	@Test(expected = IllegalStateException.class)
	public void testCloseAfterSubmit() throws Exception {
		testCloseAfterSubmit(ChannelStateWriteRequestExecutor::submit);
	}

	@Test(expected = IllegalStateException.class)
	public void testCloseAfterSubmitPriority() throws Exception {
		testCloseAfterSubmit(ChannelStateWriteRequestExecutor::submitPriority);
	}

	@Test
	public void testSubmitFailure() throws Exception {
		testSubmitFailure(ChannelStateWriteRequestExecutor::submit);
	}

	@Test
	public void testSubmitPriorityFailure() throws Exception {
		testSubmitFailure(ChannelStateWriteRequestExecutor::submitPriority);
	}

	private void testCloseAfterSubmit(BiConsumerWithException<ChannelStateWriteRequestExecutor, ChannelStateWriteRequest, Exception> requestFun) throws Exception {
		WorkerClosingDeque closingDeque = new WorkerClosingDeque();
		ChannelStateWriteRequestExecutorImpl worker = new ChannelStateWriteRequestExecutorImpl(TASK_NAME, NO_OP, closingDeque);
		closingDeque.setWorker(worker);
		TestWriteRequest request = new TestWriteRequest();
		requestFun.accept(worker, request);
		assertTrue(closingDeque.isEmpty());
		assertFalse(request.isCancelled());
	}

	private void testSubmitFailure(BiConsumerWithException<ChannelStateWriteRequestExecutor, ChannelStateWriteRequest, Exception> submitAction) throws Exception {
		TestWriteRequest request = new TestWriteRequest();
		LinkedBlockingDeque<ChannelStateWriteRequest> deque = new LinkedBlockingDeque<>();
		try {
			submitAction.accept(new ChannelStateWriteRequestExecutorImpl(TASK_NAME, NO_OP, deque), request);
		} catch (IllegalStateException e) {
			// expected: executor not started;
			return;
		} finally {
			assertTrue(request.cancelled);
			assertTrue(deque.isEmpty());
		}
		throw new RuntimeException("expected exception not thrown");
	}

	@Test
	@SuppressWarnings("CallToThreadRun")
	public void testCleanup() throws IOException {
		TestWriteRequest request = new TestWriteRequest();
		LinkedBlockingDeque<ChannelStateWriteRequest> deque = new LinkedBlockingDeque<>();
		deque.add(request);
		TestRequestDispatcher requestProcessor = new TestRequestDispatcher();
		ChannelStateWriteRequestExecutorImpl worker = new ChannelStateWriteRequestExecutorImpl(TASK_NAME, requestProcessor, deque);

		worker.close();
		worker.run();

		assertTrue(requestProcessor.isStopped());
		assertTrue(deque.isEmpty());
		assertTrue(request.isCancelled());
	}

	@Test
	public void testIgnoresInterruptsWhileRunning() throws Exception {
		TestRequestDispatcher requestProcessor = new TestRequestDispatcher();
		LinkedBlockingDeque<ChannelStateWriteRequest> deque = new LinkedBlockingDeque<>();
		try (ChannelStateWriteRequestExecutorImpl worker = new ChannelStateWriteRequestExecutorImpl(TASK_NAME, requestProcessor, deque)) {
			worker.start();
			worker.getThread().interrupt();
			worker.submit(new TestWriteRequest());
			worker.getThread().interrupt();
			while (!deque.isEmpty()) {
				Thread.sleep(100);
			}
		}
	}

	@Test
	public void testCanBeClosed() throws IOException {
		TestRequestDispatcher requestProcessor = new TestRequestDispatcher();
		try (ChannelStateWriteRequestExecutorImpl worker = new ChannelStateWriteRequestExecutorImpl(TASK_NAME, requestProcessor)) {
			worker.start();
		}
	}

	@Test
	public void testRecordsException() throws IOException {
		TestException testException = new TestException();
		TestRequestDispatcher throwingRequestProcessor = new TestRequestDispatcher() {
			@Override
			public void dispatch(ChannelStateWriteRequest request) {
				throw testException;
			}
		};
		LinkedBlockingDeque<ChannelStateWriteRequest> deque = new LinkedBlockingDeque<>(Arrays.asList(new TestWriteRequest()));
		ChannelStateWriteRequestExecutorImpl worker = new ChannelStateWriteRequestExecutorImpl(TASK_NAME, throwingRequestProcessor, deque);
		worker.run();
		try {
			worker.close();
		} catch (IOException e) {
			if (findThrowable(e, TestException.class).filter(found -> found == testException).isPresent()) {
				return;
			} else {
				throw e;
			}
		}
		fail("exception not thrown");
	}

	private static class TestWriteRequest implements ChannelStateWriteRequest {
		private boolean cancelled = false;

		@Override
		public long getCheckpointId() {
			return 0;
		}

		@Override
		public void cancel(Throwable cause) {
			cancelled = true;
		}

		public boolean isCancelled() {
			return cancelled;
		}
	}

	private static class WorkerClosingDeque extends LinkedBlockingDeque<ChannelStateWriteRequest> {
		private ChannelStateWriteRequestExecutor worker;

		@Override
		public void put(@Nonnull ChannelStateWriteRequest request) throws InterruptedException {
			super.putFirst(request);
			try {
				worker.close();
			} catch (IOException e) {
				ExceptionUtils.rethrow(e);
			}
		}

		@Override
		public void putFirst(@Nonnull ChannelStateWriteRequest request) throws InterruptedException {
			super.putFirst(request);
			try {
				worker.close();
			} catch (IOException e) {
				ExceptionUtils.rethrow(e);
			}
		}

		public void setWorker(ChannelStateWriteRequestExecutor worker) {
			this.worker = worker;
		}
	}

	private static class TestRequestDispatcher implements ChannelStateWriteRequestDispatcher {
		private boolean isStopped;

		@Override
		public void dispatch(ChannelStateWriteRequest request) {
		}

		@Override
		public void fail(Throwable cause) {
			isStopped = true;
		}

		public boolean isStopped() {
			return isStopped;
		}

	}
}

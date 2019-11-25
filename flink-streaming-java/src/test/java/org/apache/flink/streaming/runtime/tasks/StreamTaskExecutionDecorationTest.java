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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that {@link StreamTask} {@link ExecutionDecorator decorates execution} of actions that potentially needs to be synchronized.
 */
public class StreamTaskExecutionDecorationTest {
	private CountingExecutionDecorator decorator;
	private StreamTask<Object, StreamOperator<Object>> task;

	@Test
	public void testAbortCheckpointOnBarrierIsDecorated() throws Exception {
		task.abortCheckpointOnBarrier(1, null);
		verify();
	}

	@Test
	public void testTriggerCheckpointOnBarrierIsDecorated() throws Exception {
		task.triggerCheckpointOnBarrier(new CheckpointMetaData(1, 2), new CheckpointOptions(CheckpointType.CHECKPOINT, new CheckpointStorageLocationReference(new byte[]{1})), null);
		verify();
	}

	@Test
	public void testTriggerCheckpointAsyncIsDecorated() throws Exception {
		task.triggerCheckpointAsync(new CheckpointMetaData(1, 2), new CheckpointOptions(CheckpointType.CHECKPOINT, new CheckpointStorageLocationReference(new byte[]{1})), false);
		new Thread(() -> {
			try {
				Thread.sleep(50);
			} catch (InterruptedException ex) {
				ex.printStackTrace();
			} finally {
				task.mailboxProcessor.allActionsCompleted();
			}
		}).start();
		task.mailboxProcessor.runMailboxLoop();
		verify();
	}

	@Test
	public void testMailboxExecutorIsDecorated() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		task.mailboxProcessor.getMainMailboxExecutor().asExecutor("test").execute(() -> {
			try {
				verify();
			} finally {
				latch.countDown();
			}
		});
		new Thread(() -> {
			try {
				latch.await();
				task.mailboxProcessor.allActionsCompleted();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}).start();
		task.mailboxProcessor.runMailboxLoop();
	}

	@Before
	public void before() {
		decorator = new CountingExecutionDecorator();
		task = new StreamTask<Object, StreamOperator<Object>>(new StreamTaskTest.DeclineDummyEnvironment(), null, FatalExitExceptionHandler.INSTANCE, decorator) {
			@Override
			protected void init() {
			}

			@Override
			protected void processInput(MailboxDefaultAction.Controller controller) {
			}
		};
	}

	private void verify() {
		Assert.assertTrue("execution decorator was not called", decorator.wasCalled());
	}

	@After
	public void after() {
		decorator = null;
		task = null;
	}

	@SuppressWarnings("CatchMayIgnoreException")
	static class CountingExecutionDecorator implements ExecutionDecorator {
		private AtomicInteger calls = new AtomicInteger(0);

		int getCallCount() {
			return calls.get();
		}

		boolean wasCalled() {
			return getCallCount() > 0;
		}

		@Override
		public void run(Runnable runnable) {
			calls.incrementAndGet();
			try {
				runnable.run();
			} catch (Exception e) {
			}
		}

		@Override
		public <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) {
			calls.incrementAndGet();
			try {
				runnable.run();
			} catch (Throwable e) {
			}
		}

		@Override
		public <R> R call(Callable<R> callable) {
			calls.incrementAndGet();
			try {
				return callable.call();
			} catch (Exception e) {
				return null;
			}
		}

		@Override
		public void dispatch(Mail mail) {
			calls.incrementAndGet();
			try {
				mail.run();
			} catch (Exception e) {
			}
		}
	}
}

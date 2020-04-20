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
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that {@link StreamTask} {@link StreamTaskActionExecutor decorates execution} of actions that potentially needs to be synchronized.
 */
public class StreamTaskExecutionDecorationTest {
	private CountingStreamTaskActionExecutor decorator;
	private StreamTask<Object, StreamOperator<Object>> task;
	private TaskMailboxImpl mailbox;

	@Test
	public void testAbortCheckpointOnBarrierIsDecorated() throws Exception {
		task.abortCheckpointOnBarrier(1, null);
		Assert.assertTrue("execution decorator was not called", decorator.wasCalled());
	}

	@Test
	public void testTriggerCheckpointOnBarrierIsDecorated() throws Exception {
		task.triggerCheckpointOnBarrier(new CheckpointMetaData(1, 2), new CheckpointOptions(CheckpointType.CHECKPOINT, new CheckpointStorageLocationReference(new byte[]{1})), null);
		Assert.assertTrue("execution decorator was not called", decorator.wasCalled());
	}

	@Test
	public void testTriggerCheckpointAsyncIsDecorated() {
		task.triggerCheckpointAsync(new CheckpointMetaData(1, 2), new CheckpointOptions(CheckpointType.CHECKPOINT, new CheckpointStorageLocationReference(new byte[]{1})), false);
		Assert.assertTrue("mailbox is empty", mailbox.hasMail());
		Assert.assertFalse("execution decorator was called preliminary", decorator.wasCalled());
		mailbox.drain().forEach(m -> {
			try {
				m.run();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		Assert.assertTrue("execution decorator was not called", decorator.wasCalled());
	}

	@Test
	public void testMailboxExecutorIsDecorated() throws Exception {
		task.mailboxProcessor.getMainMailboxExecutor().execute(() -> task.mailboxProcessor.allActionsCompleted(), "");
		task.mailboxProcessor.runMailboxLoop();
		Assert.assertTrue("execution decorator was not called", decorator.wasCalled());
	}

	@Before
	public void before() throws Exception {
		mailbox = new TaskMailboxImpl();
		decorator = new CountingStreamTaskActionExecutor();
		task = new StreamTask<Object, StreamOperator<Object>>(new StreamTaskTest.DeclineDummyEnvironment(), null, FatalExitExceptionHandler.INSTANCE, decorator, mailbox) {
			@Override
			protected void init() {
			}

			@Override
			protected void processInput(MailboxDefaultAction.Controller controller) {
			}
		};
		task.operatorChain = new OperatorChain<>(task, new NonRecordWriter<>());
	}

	@After
	public void after() {
		decorator = null;
		task = null;
	}

	static class CountingStreamTaskActionExecutor extends StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor {
		private final AtomicInteger calls = new AtomicInteger(0);

		CountingStreamTaskActionExecutor() {
			super(new Object());
		}

		int getCallCount() {
			return calls.get();
		}

		boolean wasCalled() {
			return getCallCount() > 0;
		}

		@Override
		public void run(RunnableWithException runnable) throws Exception {
			calls.incrementAndGet();
			runnable.run();
		}

		@Override
		public <E extends Throwable> void runThrowing(ThrowingRunnable<E> runnable) throws E {
			calls.incrementAndGet();
			runnable.run();
		}

		@Override
		public <R> R call(Callable<R> callable) throws Exception {
			calls.incrementAndGet();
			return callable.call();
		}
	}
}

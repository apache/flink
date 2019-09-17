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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.execution.MailboxExecutor;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test to verify that timer triggers are run according to operator precedence (combined with yield() at operator level).
 */
public class StreamTaskOperatorTimerTest extends TestLogger {
	private static List<String> events;

	@Test
	public void testOperatorYieldExecutesSelectedTimers() throws Exception {
		events = new ArrayList<>();
		final OneInputStreamTaskTestHarness<Integer, Integer> testHarness = new OneInputStreamTaskTestHarness<>(
				OneInputStreamTask::new,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setupOperatorChain(new OperatorID(), new TestOperatorFactory<>())
				.chain(new OperatorID(), new TestOperatorFactory<>(), IntSerializer.INSTANCE)
				.finish();

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<>(42));

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		assertThat(events, is(Arrays.asList("Timer:1:0", "Timer:0:0")));
	}

	private static class TestOperatorFactory<T> implements OneInputStreamOperatorFactory<T, T>, YieldingOperatorFactory<T> {
		private MailboxExecutor mailboxExecutor;

		@Override
		public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
			this.mailboxExecutor = mailboxExecutor;
		}

		@Override
		public <Operator extends StreamOperator<T>> Operator createStreamOperator(
				StreamTask<?, ?> containingTask,
				StreamConfig config,
				Output<StreamRecord<T>> output) {
			TestOperator<T> operator = new TestOperator<>(config.getChainIndex(), mailboxExecutor);
			operator.setup(containingTask, config, output);
			return (Operator) operator;
		}

		@Override
		public void setChainingStrategy(ChainingStrategy strategy) {
		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return ChainingStrategy.ALWAYS;
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			return TestOperator.class;
		}
	}

	private static class TestOperator<T>
			extends AbstractStreamOperator<T>
			implements OneInputStreamOperator<T, T> {

		private final transient MailboxExecutor mailboxExecutor;
		private final int chainIndex;
		private transient int count;

		TestOperator(int chainIndex, MailboxExecutor mailboxExecutor) {
			this.chainIndex = chainIndex;
			this.mailboxExecutor = mailboxExecutor;
		}

		@Override
		public void processElement(StreamRecord<T> element) throws Exception {
			// The test operator creates a one-time timer (per input element) and passes the input element further
			// (to the next operator or to the output).
			// The execution is yielded until the operator's timer trigger is confirmed.

			int index = count;
			ProcessingTimeService processingTimeService = getProcessingTimeService();
			processingTimeService
				.registerTimer(
					processingTimeService.getCurrentProcessingTime() + 1000L,
					timestamp -> {
						events.add("Timer:" + chainIndex + ":" + index);
						--count;
					});

			++count;
			output.collect(element);

			while (count > 0) {
				mailboxExecutor.yield();
			}
		}
	}
}

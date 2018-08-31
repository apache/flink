/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StoppableStreamSource;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * These tests verify that the RichFunction methods are called (in correct order). And that
 * checkpointing/element emission don't occur concurrently.
 */
public class SourceStreamTaskStoppingTest {

	// test flag for testStop()
	static boolean stopped = false;

	@Test
	public void testStop() {
		final StoppableSourceStreamTask<Object, StoppableSource> sourceTask =
				new StoppableSourceStreamTask<>(new DummyEnvironment("test", 1, 0));

		sourceTask.headOperator = new StoppableStreamSource<>(new StoppableSource());

		sourceTask.stop();

		assertTrue(stopped);
	}

	@Test
	public void testStopBeforeInitialization() throws Exception {

		final StoppableSourceStreamTask<Object, StoppableFailingSource> sourceTask =
				new StoppableSourceStreamTask<>(new DummyEnvironment("test", 1, 0));
		sourceTask.stop();

		sourceTask.headOperator = new StoppableStreamSource<>(new StoppableFailingSource());
		sourceTask.run();
	}

	// ------------------------------------------------------------------------

	private static class StoppableSource extends RichSourceFunction<Object> implements StoppableFunction {
		private static final long serialVersionUID = 728864804042338806L;

		@Override
		public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Object> ctx)
				throws Exception {
		}

		@Override
		public void cancel() {}

		@Override
		public void stop() {
			stopped = true;
		}
	}

	private static class StoppableFailingSource extends RichSourceFunction<Object> implements StoppableFunction {
		private static final long serialVersionUID = 728864804042338806L;

		@Override
		public void run(SourceContext<Object> ctx) throws Exception {
			fail("should not be called");
		}

		@Override
		public void cancel() {}

		@Override
		public void stop() {}
	}
}


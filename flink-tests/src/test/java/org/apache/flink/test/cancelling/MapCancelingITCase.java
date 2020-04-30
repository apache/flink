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

package org.apache.flink.test.cancelling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.InfiniteIntegerInputFormat;

import org.junit.Test;

/**
 * Test job cancellation from within a MapFunction.
 */
public class MapCancelingITCase extends CancelingTestBase {

	@Test
	public void testMapCancelling() throws Exception {
		executeTask(new IdentityMapper<Integer>());
	}

	@Test
	public void testSlowMapCancelling() throws Exception {
		executeTask(new DelayingIdentityMapper<Integer>());
	}

	@Test
	public void testMapWithLongCancellingResponse() throws Exception {
		executeTask(new LongCancelTimeIdentityMapper<Integer>());
	}

	@Test
	public void testMapPriorToFirstRecordReading() throws Exception {
		executeTask(new StuckInOpenIdentityMapper<Integer>());
	}

	public void executeTask(MapFunction<Integer, Integer> mapper) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env
				.createInput(new InfiniteIntegerInputFormat(false))
				.map(mapper)
				.output(new DiscardingOutputFormat<Integer>());

		env.setParallelism(PARALLELISM);

		runAndCancelJob(env.createProgramPlan(), 5 * 1000, 10 * 1000);
	}

	// --------------------------------------------------------------------------------------------

	private static final class IdentityMapper<IN> implements MapFunction<IN, IN> {
		private static final long serialVersionUID = 1L;

		@Override
		public IN map(IN value) throws Exception {
			return value;
		}
	}

	private static final class DelayingIdentityMapper<IN> implements MapFunction<IN, IN> {
		private static final long serialVersionUID = 1L;

		private static final int WAIT_TIME_PER_VALUE = 10 * 1000; // 10 sec.

		@Override
		public IN map(IN value) throws Exception {
			Thread.sleep(WAIT_TIME_PER_VALUE);
			return value;
		}
	}

	private static final class LongCancelTimeIdentityMapper<IN> implements MapFunction<IN, IN> {
		private static final long serialVersionUID = 1L;

		private static final int WAIT_TIME_PER_VALUE = 5 * 1000; // 5 sec.

		@Override
		public IN map(IN value) throws Exception {
			final long start = System.currentTimeMillis();
			long remaining = WAIT_TIME_PER_VALUE;
			do {
				try {
					Thread.sleep(remaining);
				} catch (InterruptedException iex) {
				}
			} while ((remaining = WAIT_TIME_PER_VALUE - System.currentTimeMillis() + start) > 0);

			return value;
		}
	}

	private static final class StuckInOpenIdentityMapper<IN> extends RichMapFunction<IN, IN> {
		private static final long serialVersionUID = 1L;

		@Override
		public void open(Configuration parameters) throws Exception {
			synchronized (this) {
				wait();
			}
		}

		@Override
		public IN map(IN value) throws Exception {
			return value;
		}
	}
}

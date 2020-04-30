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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.InfiniteIntegerTupleInputFormat;
import org.apache.flink.test.util.UniformIntTupleGeneratorInputFormat;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * Test job cancellation from within a JoinFunction.
 */
@Ignore("Takes too long.")
public class JoinCancelingITCase extends CancelingTestBase {

	// --------------- Test Sort Matches that are canceled while still reading / sorting -----------------
	private void executeTask(JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> joiner, boolean slow) throws Exception {
		executeTask(joiner, slow, PARALLELISM);
	}

	private void executeTask(JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> joiner, boolean slow, int parallelism) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<Integer, Integer>> input1 = env.createInput(new InfiniteIntegerTupleInputFormat(slow));
		DataSet<Tuple2<Integer, Integer>> input2 = env.createInput(new InfiniteIntegerTupleInputFormat(slow));

		input1.join(input2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
				.where(0)
				.equalTo(0)
				.with(joiner)
				.output(new DiscardingOutputFormat<Tuple2<Integer, Integer>>());

		env.setParallelism(parallelism);

		runAndCancelJob(env.createProgramPlan(), 5 * 1000, 10 * 1000);
	}

	@Test
	public void testCancelSortMatchWhileReadingSlowInputs() throws Exception {
		executeTask(new SimpleMatcher<Integer>(), true);
	}

	@Test
	public void testCancelSortMatchWhileReadingFastInputs() throws Exception {
		executeTask(new SimpleMatcher<Integer>(), false);
	}

	@Test
	public void testCancelSortMatchPriorToFirstRecordReading() throws Exception {
		executeTask(new StuckInOpenMatcher<Integer>(), false);
	}

	private void executeTaskWithGenerator(
			JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> joiner,
			int keys, int vals, int msecsTillCanceling, int maxTimeTillCanceled) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<Integer, Integer>> input1 = env.createInput(new UniformIntTupleGeneratorInputFormat(keys, vals));
		DataSet<Tuple2<Integer, Integer>> input2 = env.createInput(new UniformIntTupleGeneratorInputFormat(keys, vals));

		input1.join(input2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
				.where(0)
				.equalTo(0)
				.with(joiner)
				.output(new DiscardingOutputFormat<Tuple2<Integer, Integer>>());

		env.setParallelism(PARALLELISM);

		runAndCancelJob(env.createProgramPlan(), msecsTillCanceling, maxTimeTillCanceled);
	}

	@Test
	public void testCancelSortMatchWhileDoingHeavySorting() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		HeavyCompareGeneratorInputFormat input = new HeavyCompareGeneratorInputFormat(100);
		DataSet<Tuple2<HeavyCompare, Integer>> input1 = env.createInput(input);
		DataSet<Tuple2<HeavyCompare, Integer>> input2 = env.createInput(input);

		input1.join(input2, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
				.where(0)
				.equalTo(0)
				.with(new JoinFunction<Tuple2<HeavyCompare, Integer>, Tuple2<HeavyCompare, Integer>, Tuple2<HeavyCompare, Integer>>() {
					@Override
					public Tuple2<HeavyCompare, Integer> join(
						Tuple2<HeavyCompare, Integer> first,
						Tuple2<HeavyCompare, Integer> second) throws Exception {
						throw new Exception("Job should be canceled in sort-merge phase, never run here ...");
					}
				})
				.output(new DiscardingOutputFormat<Tuple2<HeavyCompare, Integer>>());

		runAndCancelJob(env.createProgramPlan(), 30 * 1000, 60 * 1000);
	}

	// --------------- Test Sort Matches that are canceled while in the Matching Phase -----------------

	@Test
	public void testCancelSortMatchWhileJoining() throws Exception {
		executeTaskWithGenerator(new DelayingMatcher<Integer>(), 500, 3, 10 * 1000, 20 * 1000);
	}

	@Test
	public void testCancelSortMatchWithLongCancellingResponse() throws Exception {
		executeTaskWithGenerator(new LongCancelTimeMatcher<Integer>(), 500, 3, 10 * 1000, 10 * 1000);
	}

	// -------------------------------------- Test System corner cases ---------------------------------

	@Test
	public void testCancelSortMatchWithHighparallelism() throws Exception {
		executeTask(new SimpleMatcher<Integer>(), false, 64);
	}

	// --------------------------------------------------------------------------------------------

	private static final class SimpleMatcher<IN> implements JoinFunction<Tuple2<IN, IN>, Tuple2<IN, IN>, Tuple2<IN, IN>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<IN, IN> join(Tuple2<IN, IN> first, Tuple2<IN, IN> second) throws Exception {
			return new Tuple2<>(first.f0, second.f0);
		}
	}

	private static final class DelayingMatcher<IN> implements JoinFunction<Tuple2<IN, IN>, Tuple2<IN, IN>, Tuple2<IN, IN>> {
		private static final long serialVersionUID = 1L;

		private static final int WAIT_TIME_PER_RECORD = 10 * 1000; // 10 sec.

		@Override
		public Tuple2<IN, IN> join(Tuple2<IN, IN> first, Tuple2<IN, IN> second) throws Exception {
			Thread.sleep(WAIT_TIME_PER_RECORD);
			return new Tuple2<>(first.f0, second.f0);
		}
	}

	private static final class LongCancelTimeMatcher<IN> implements JoinFunction<Tuple2<IN, IN>, Tuple2<IN, IN>, Tuple2<IN, IN>> {
		private static final long serialVersionUID = 1L;

		private static final int WAIT_TIME_PER_RECORD = 5 * 1000; // 5 sec.

		@Override
		public Tuple2<IN, IN> join(Tuple2<IN, IN> first, Tuple2<IN, IN> second) throws Exception {
			final long start = System.currentTimeMillis();
			long remaining = WAIT_TIME_PER_RECORD;
			do {
				try {
					Thread.sleep(remaining);
				} catch (InterruptedException iex) {}
			} while ((remaining = WAIT_TIME_PER_RECORD - System.currentTimeMillis() + start) > 0);
			return new Tuple2<>(first.f0, second.f0);
		}
	}

	private static final class StuckInOpenMatcher<IN> extends RichJoinFunction<Tuple2<IN, IN>, Tuple2<IN, IN>, Tuple2<IN, IN>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void open(Configuration parameters) throws Exception {
			synchronized (this) {
				wait();
			}
		}

		@Override
		public Tuple2<IN, IN> join(Tuple2<IN, IN> first, Tuple2<IN, IN> second) throws Exception {
			return new Tuple2<>(first.f0, second.f0);
		}
	}
}

class HeavyCompare implements Comparable<HeavyCompare>, java.io.Serializable {
	@Override
	public int compareTo(org.apache.flink.test.cancelling.HeavyCompare o) {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException iex) {}
		return 0;
	}
}

class HeavyCompareGeneratorInputFormat extends GenericInputFormat<Tuple2<HeavyCompare, Integer>> {
	private int valueTotal;

	public HeavyCompareGeneratorInputFormat(int numVals) {
		this.valueTotal = numVals;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return valueTotal <= 0;
	}

	@Override
	public Tuple2<HeavyCompare, Integer> nextRecord(Tuple2<HeavyCompare, Integer> reuse) throws IOException {
		valueTotal -= 1;
		return new Tuple2<>(new HeavyCompare(), 20110701);
	}
}

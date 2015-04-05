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

package org.apache.flink.test.iterative.accumulators;

import java.util.Random;

import org.apache.flink.api.common.accumulators.ConvergenceCriterion;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the functionality of aggregators in bulk and delta iterative cases.
 */
@RunWith(Parameterized.class)
public class IterationAccumulatorsITCase extends MultipleProgramsTestBase {

	private static final int MAX_ITERATIONS = 20;
	private static final int parallelism = 2;
	private static final String NEGATIVE_ELEMENTS_AGGR = "count.negative.elements";

	public IterationAccumulatorsITCase(TestExecutionMode mode){
		super(mode);
	}

	private String resultPath;
	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testAggregatorWithoutParameterForIterate() throws Exception {
		/*
		 * Test aggregator without parameter for iterate
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		DataSet<Integer> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env);
		IterativeDataSet<Integer> iteration = initialSolutionSet.iterate(MAX_ITERATIONS);

		DataSet<Integer> updatedDs = iteration.map(new SubtractOneMap());
		iteration.closeWith(updatedDs, 
				new NegativeElementsConvergenceCriterion(), 
				NEGATIVE_ELEMENTS_AGGR).writeAsText(resultPath);
		env.execute();

		expected =  "-3\n" + "-2\n" + "-2\n" + "-1\n" + "-1\n"
				+ "-1\n" + "0\n" + "0\n" + "0\n" + "0\n"
				+ "1\n" + "1\n" + "1\n" + "1\n" + "1\n";
	}

	@Test
	public void testAggregatorWithParameterForIterate() throws Exception {
		/*
		 * Test aggregator with parameter for iterate
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		DataSet<Integer> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env);
		IterativeDataSet<Integer> iteration = initialSolutionSet.iterate(MAX_ITERATIONS);

		DataSet<Integer> updatedDs = iteration.map(new SubtractOneMapWithParam());
		iteration.closeWith(updatedDs,
				new NegativeElementsConvergenceCriterion(), 
				NEGATIVE_ELEMENTS_AGGR).writeAsText(resultPath);
		env.execute();

		expected =  "-3\n" + "-2\n" + "-2\n" + "-1\n" + "-1\n"
				+ "-1\n" + "0\n" + "0\n" + "0\n" + "0\n"
				+ "1\n" + "1\n" + "1\n" + "1\n" + "1\n";
	}

	@Test
	public void testConvergenceCriterionWithParameterForIterate() throws Exception {
		/*
		 * Test convergence criterion with parameter for iterate
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		DataSet<Integer> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env);
		IterativeDataSet<Integer> iteration = initialSolutionSet.iterate(MAX_ITERATIONS);
		
		DataSet<Integer> updatedDs = iteration.map(new SubtractOneMap());
		iteration.closeWith(updatedDs, 
				new NegativeElementsConvergenceCriterionWithParam(3),
				NEGATIVE_ELEMENTS_AGGR).writeAsText(resultPath);
		env.execute();

		expected = "-3\n" + "-2\n" + "-2\n" + "-1\n" + "-1\n"
				+ "-1\n" + "0\n" + "0\n" + "0\n" + "0\n"
				+ "1\n" + "1\n" + "1\n" + "1\n" + "1\n";
	}

	@Test
	public void testAggregatorWithoutParameterForIterateDelta() throws Exception {
		/*
		 * Test aggregator without parameter for iterateDelta
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		DataSet<Tuple2<Integer, Integer>> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env).map(new TupleMakerMap());

		DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration = initialSolutionSet.iterateDelta(
				initialSolutionSet, MAX_ITERATIONS, 0);

		DataSet<Tuple2<Integer, Integer>> updatedDs = iteration.getWorkset().map(new AggregateMapDelta());

		DataSet<Tuple2<Integer, Integer>> newElements = updatedDs.join(iteration.getSolutionSet())
				.where(0).equalTo(0).flatMap(new UpdateFilter());

		DataSet<Tuple2<Integer, Integer>> iterationRes = iteration.closeWith(newElements, newElements);
		DataSet<Integer> result = iterationRes.map(new ProjectSecondMapper());
		result.writeAsText(resultPath);

		env.execute();

		expected = "1\n" + "2\n" + "2\n" + "3\n" + "3\n"
				+ "3\n" + "4\n" + "4\n" + "4\n" + "4\n"
				+ "5\n" + "5\n" + "5\n" + "5\n" + "5\n";
	}

	@Test
	public void testAggregatorWithParameterForIterateDelta() throws Exception {
		/*
		 * Test aggregator with parameter for iterateDelta
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		DataSet<Tuple2<Integer, Integer>> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env).map(new TupleMakerMap());

		DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration = initialSolutionSet.iterateDelta(
				initialSolutionSet, MAX_ITERATIONS, 0);

		DataSet<Tuple2<Integer, Integer>> updatedDs = iteration.getWorkset().map(new AggregateMapDelta());

		DataSet<Tuple2<Integer, Integer>> newElements = updatedDs.join(iteration.getSolutionSet())
				.where(0).equalTo(0).flatMap(new UpdateFilter());

		DataSet<Tuple2<Integer, Integer>> iterationRes = iteration.closeWith(newElements, newElements);
		DataSet<Integer> result = iterationRes.map(new ProjectSecondMapper());
		result.writeAsText(resultPath);

		env.execute();

		expected = "1\n" + "2\n" + "2\n" + "3\n" + "3\n"
				+ "3\n" + "4\n" + "4\n" + "4\n" + "4\n"
				+ "5\n" + "5\n" + "5\n" + "5\n" + "5\n";
	}

	@SuppressWarnings("serial")
	public static final class NegativeElementsConvergenceCriterion implements ConvergenceCriterion<Long> {

		@Override
		public boolean isConverged(int iteration, Long value) {
			return value.longValue() > 3;
		}
	}

	@SuppressWarnings("serial")
	public static final class NegativeElementsConvergenceCriterionWithParam implements ConvergenceCriterion<Long> {

		private int value;

		public NegativeElementsConvergenceCriterionWithParam(int val) {
			this.value = val;
		}

		public int getValue() {
			return this.value;
		}

		@Override
		public boolean isConverged(int iteration, Long value) {
			return value.longValue() > this.value;
		}
	}

	@SuppressWarnings("serial")
	public static final class SubtractOneMap extends RichMapFunction<Integer, Integer> {

		private LongCounter  aggr;

		@Override
		public void open(Configuration conf) {
			aggr = new LongCounter();
			getIterationRuntimeContext().addIterationAccumulator(NEGATIVE_ELEMENTS_AGGR, aggr);
		}

		@Override
		public Integer map(Integer value) {
			Integer newValue = Integer.valueOf(value.intValue() - 1);
			// count negative numbers
			if (newValue.intValue() < 0) {
				aggr.add(1l);
			}
			return newValue;
		}
	}

	@SuppressWarnings("serial")
	public static final class SubtractOneMapWithParam extends RichMapFunction<Integer, Integer> {

		private LongSumAggregatorWithParameter aggr;

		@Override
		public void open(Configuration conf) {
			aggr = new LongSumAggregatorWithParameter(0);
			getIterationRuntimeContext().addIterationAccumulator(NEGATIVE_ELEMENTS_AGGR, aggr);
		}

		@Override
		public Integer map(Integer value) {
			Integer newValue = Integer.valueOf(value.intValue() - 1);
			// count numbers less then the aggregator parameter
			if ( newValue.intValue() < aggr.getValue() ) {
				aggr.add(1l);
			}
			return newValue;
		}
	}

	@SuppressWarnings("serial")
	public static class LongSumAggregatorWithParameter extends LongCounter {

		private int value;
		
		public LongSumAggregatorWithParameter() {
			
		}

		public LongSumAggregatorWithParameter(int val) {
			this.value = val;
		}

		public int getValue() {
			return this.value;
		}
	}

	@SuppressWarnings("serial")
	public static final class TupleMakerMap extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

		private Random rnd;

		@Override
		public void open(Configuration parameters){
			rnd = new Random(0xC0FFEBADBEEFDEADL + getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public Tuple2<Integer, Integer> map(Integer value) {
			Integer nodeId = Integer.valueOf(rnd.nextInt(100000));
			return new Tuple2<Integer, Integer>(nodeId, value);
		}

	}

	@SuppressWarnings("serial")
	public static final class AggregateMapDelta extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		private LongCounter aggr;
		private Long previousAggr;
		private int superstep;

		@Override
		public void open(Configuration conf) {
			aggr = new LongCounter();
			getIterationRuntimeContext().addIterationAccumulator(NEGATIVE_ELEMENTS_AGGR, aggr);
			
			superstep = getIterationRuntimeContext().getSuperstepNumber();

			if (superstep > 1) {
				previousAggr = (Long) getIterationRuntimeContext().getPreviousIterationAccumulator(NEGATIVE_ELEMENTS_AGGR).getLocalValue();
				// check previous aggregator value
				Assert.assertEquals(superstep - 1, previousAggr.longValue());
			}

		}

		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) {
			// count the elements that are equal to the superstep number
			if (value.f1.intValue() == superstep) {
				aggr.add(1l);
			}
			return value;
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFilter extends RichFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
			Tuple2<Integer, Integer>> {

		private int superstep;

		@Override
		public void open(Configuration conf) {
			superstep = getIterationRuntimeContext().getSuperstepNumber();
		}

		@Override
		public void flatMap(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> value,
				Collector<Tuple2<Integer, Integer>> out) {

			if (value.f0.f1  > superstep) {
				out.collect(value.f0);
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class ProjectSecondMapper extends RichMapFunction<Tuple2<Integer, Integer>, Integer> {

		@Override
		public Integer map(Tuple2<Integer, Integer> value) {
			return value.f1;
		}
	}
}

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

package org.apache.flink.test.iterative.aggregators;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Test the functionality of aggregators in bulk and delta iterative cases.
 */
@RunWith(Parameterized.class)
public class AggregatorsITCase extends MultipleProgramsTestBase {

	private static final int MAX_ITERATIONS = 20;
	private static final int parallelism = 2;
	private static final String NEGATIVE_ELEMENTS_AGGR = "count.negative.elements";

	public AggregatorsITCase(TestExecutionMode mode){
		super(mode);
	}

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testDistributedCacheWithIterations() throws Exception{
		final String testString = "Et tu, Brute?";
		final String testName = "testing_caesar";

		final File folder = tempFolder.newFolder();
		final File resultFile = new File(folder, UUID.randomUUID().toString());

		String testPath = resultFile.toString();
		String resultPath = resultFile.toURI().toString();

		File tempFile = new File(testPath);
		try (FileWriter writer = new FileWriter(tempFile)) {
			writer.write(testString);
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.registerCachedFile(resultPath, testName);

		IterativeDataSet<Long> solution = env.fromElements(1L).iterate(2);
		solution.closeWith(env.generateSequence(1, 2).filter(new RichFilterFunction<Long>() {
			@Override
			public void open(Configuration parameters) throws Exception{
				File file = getRuntimeContext().getDistributedCache().getFile(testName);
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String output = reader.readLine();
				reader.close();
				assertEquals(output, testString);
			}

			@Override
			public boolean filter(Long value) throws Exception {
				return false;
			}
		}).withBroadcastSet(solution, "SOLUTION")).output(new DiscardingOutputFormat<Long>());
		env.execute();
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

		// register aggregator
		LongSumAggregator aggr = new LongSumAggregator();
		iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);

		// register convergence criterion
		iteration.registerAggregationConvergenceCriterion(NEGATIVE_ELEMENTS_AGGR, aggr,
				new NegativeElementsConvergenceCriterion());

		DataSet<Integer> updatedDs = iteration.map(new SubtractOneMap());
		List<Integer> result = iteration.closeWith(updatedDs).collect();
		Collections.sort(result);

		List<Integer> expected = Arrays.asList(-3, -2, -2, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 1);

		assertEquals(expected, result);
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

		// register aggregator
		LongSumAggregatorWithParameter aggr = new LongSumAggregatorWithParameter(0);
		iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);

		// register convergence criterion
		iteration.registerAggregationConvergenceCriterion(NEGATIVE_ELEMENTS_AGGR, aggr,
				new NegativeElementsConvergenceCriterion());

		DataSet<Integer> updatedDs = iteration.map(new SubtractOneMapWithParam());
		List<Integer> result = iteration.closeWith(updatedDs).collect();
		Collections.sort(result);

		List<Integer> expected = Arrays.asList(-3, -2, -2, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 1);

		assertEquals(expected, result);
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

		// register aggregator
		LongSumAggregator aggr = new LongSumAggregator();
		iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);

		// register convergence criterion
		iteration.registerAggregationConvergenceCriterion(NEGATIVE_ELEMENTS_AGGR, aggr,
				new NegativeElementsConvergenceCriterionWithParam(3));

		DataSet<Integer> updatedDs = iteration.map(new SubtractOneMap());
		List<Integer> result = iteration.closeWith(updatedDs).collect();
		Collections.sort(result);

		List<Integer> expected = Arrays.asList(-3, -2, -2, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 1);

		assertEquals(expected, result);
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

		// register aggregator
		LongSumAggregator aggr = new LongSumAggregator();
		iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);

		DataSet<Tuple2<Integer, Integer>> updatedDs = iteration.getWorkset().map(new AggregateMapDelta());

		DataSet<Tuple2<Integer, Integer>> newElements = updatedDs.join(iteration.getSolutionSet())
				.where(0).equalTo(0).flatMap(new UpdateFilter());

		DataSet<Tuple2<Integer, Integer>> iterationRes = iteration.closeWith(newElements, newElements);
		List<Integer> result = iterationRes.map(new ProjectSecondMapper()).collect();
		Collections.sort(result);

		List<Integer> expected = Arrays.asList(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5);

		assertEquals(expected, result);
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

		// register aggregator
		LongSumAggregator aggr = new LongSumAggregatorWithParameter(4);
		iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);

		DataSet<Tuple2<Integer, Integer>> updatedDs = iteration.getWorkset().map(new AggregateMapDelta());

		DataSet<Tuple2<Integer, Integer>> newElements = updatedDs.join(iteration.getSolutionSet())
				.where(0).equalTo(0).flatMap(new UpdateFilter());

		DataSet<Tuple2<Integer, Integer>> iterationRes = iteration.closeWith(newElements, newElements);
		List<Integer> result = iterationRes.map(new ProjectSecondMapper()).collect();
		Collections.sort(result);

		List<Integer> expected = Arrays.asList(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5);

		assertEquals(result, expected);
	}

	@Test
	public void testConvergenceCriterionWithParameterForIterateDelta() throws Exception {
		/*
		 * Test convergence criterion with parameter for iterate delta
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		DataSet<Tuple2<Integer, Integer>> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env).map(new TupleMakerMap());

		DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration = initialSolutionSet.iterateDelta(
				initialSolutionSet, MAX_ITERATIONS, 0);

		// register aggregator
		LongSumAggregator aggr = new LongSumAggregator();
		iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);

		// register convergence criterion
		iteration.registerAggregationConvergenceCriterion(NEGATIVE_ELEMENTS_AGGR, aggr,
				new NegativeElementsConvergenceCriterionWithParam(3));

		DataSet<Tuple2<Integer, Integer>> updatedDs = iteration.getWorkset().map(new AggregateAndSubtractOneDelta());

		DataSet<Tuple2<Integer, Integer>> newElements = updatedDs.join(iteration.getSolutionSet())
				.where(0).equalTo(0).projectFirst(0, 1);

		DataSet<Tuple2<Integer, Integer>> iterationRes = iteration.closeWith(newElements, newElements);
		List<Integer> result = iterationRes.map(new ProjectSecondMapper()).collect();
		Collections.sort(result);

		List<Integer> expected = Arrays.asList(-3, -2, -2, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 1);

		assertEquals(expected, result);
	}

	@SuppressWarnings("serial")
	private static final class NegativeElementsConvergenceCriterion implements ConvergenceCriterion<LongValue> {

		@Override
		public boolean isConverged(int iteration, LongValue value) {
			return value.getValue() > 3;
		}
	}

	@SuppressWarnings("serial")
	private static final class NegativeElementsConvergenceCriterionWithParam implements ConvergenceCriterion<LongValue> {

		private int value;

		public NegativeElementsConvergenceCriterionWithParam(int val) {
			this.value = val;
		}

		public int getValue() {
			return this.value;
		}

		@Override
		public boolean isConverged(int iteration, LongValue value) {
			return value.getValue() > this.value;
		}
	}

	@SuppressWarnings("serial")
	private static final class SubtractOneMap extends RichMapFunction<Integer, Integer> {

		private LongSumAggregator aggr;

		@Override
		public void open(Configuration conf) {

			aggr = getIterationRuntimeContext().getIterationAggregator(NEGATIVE_ELEMENTS_AGGR);
		}

		@Override
		public Integer map(Integer value) {
			Integer newValue = value - 1;
			// count negative numbers
			if (newValue < 0) {
				aggr.aggregate(1L);
			}
			return newValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class SubtractOneMapWithParam extends RichMapFunction<Integer, Integer> {

		private LongSumAggregatorWithParameter aggr;

		@Override
		public void open(Configuration conf) {
			aggr = getIterationRuntimeContext().getIterationAggregator(NEGATIVE_ELEMENTS_AGGR);
		}

		@Override
		public Integer map(Integer value) {
			Integer newValue = value - 1;
			// count numbers less than the aggregator parameter
			if (newValue < aggr.getValue()) {
				aggr.aggregate(1L);
			}
			return newValue;
		}
	}

	@SuppressWarnings("serial")
	private static class LongSumAggregatorWithParameter extends LongSumAggregator {

		private int value;

		public LongSumAggregatorWithParameter(int val) {
			this.value = val;
		}

		public int getValue() {
			return this.value;
		}
	}

	@SuppressWarnings("serial")
	private static final class TupleMakerMap extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

		private Random rnd;

		@Override
		public void open(Configuration parameters){
			rnd = new Random(0xC0FFEBADBEEFDEADL + getRuntimeContext().getIndexOfThisSubtask());
		}

		@Override
		public Tuple2<Integer, Integer> map(Integer value) {
			Integer nodeId = rnd.nextInt(100000);
			return new Tuple2<>(nodeId, value);
		}

	}

	@SuppressWarnings("serial")
	private static final class AggregateMapDelta extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		private LongSumAggregator aggr;
		private LongValue previousAggr;
		private int superstep;

		@Override
		public void open(Configuration conf) {
			aggr = getIterationRuntimeContext().getIterationAggregator(NEGATIVE_ELEMENTS_AGGR);
			superstep = getIterationRuntimeContext().getSuperstepNumber();

			if (superstep > 1) {
				previousAggr = getIterationRuntimeContext().getPreviousIterationAggregate(NEGATIVE_ELEMENTS_AGGR);
				// check previous aggregator value
				Assert.assertEquals(superstep - 1, previousAggr.getValue());
			}

		}

		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) {
			// count the elements that are equal to the superstep number
			if (value.f1 == superstep) {
				aggr.aggregate(1L);
			}
			return value;
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateFilter extends RichFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>,
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
	private static final class ProjectSecondMapper extends RichMapFunction<Tuple2<Integer, Integer>, Integer> {

		@Override
		public Integer map(Tuple2<Integer, Integer> value) {
			return value.f1;
		}
	}

	@SuppressWarnings("serial")
	private static final class AggregateAndSubtractOneDelta extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		private LongSumAggregator aggr;
		private LongValue previousAggr;
		private int superstep;

		@Override
		public void open(Configuration conf) {
			aggr = getIterationRuntimeContext().getIterationAggregator(NEGATIVE_ELEMENTS_AGGR);
			superstep = getIterationRuntimeContext().getSuperstepNumber();

			if (superstep > 1) {
				previousAggr = getIterationRuntimeContext().getPreviousIterationAggregate(NEGATIVE_ELEMENTS_AGGR);
				// check previous aggregator value
				Assert.assertEquals(superstep - 1, previousAggr.getValue());
			}

		}

		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) {
			// count the ones
			if (value.f1 == 1) {
				aggr.aggregate(1L);
			}
			value.f1--;
			return value;
		}
	}

}

/**
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Random;

import org.junit.Assert;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.DeltaIteration;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.IterativeDataSet;

/**
 * Test the functionality of aggregators in bulk and delta iterative cases.
 *
 */
@RunWith(Parameterized.class)
public class AggregatorsITCase extends JavaProgramTestBase {

	private static final int NUM_PROGRAMS = 5;
	private static final int MAX_ITERATIONS = 20;	
	private static final int DOP = 2;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	public AggregatorsITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = AggregatorProgs.runProgram(curProgId, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {

		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}

		return toParameterList(tConfigs);
	}

	private static class AggregatorProgs {

		private static final String NEGATIVE_ELEMENTS_AGGR = "count.negative.elements";

		public static String runProgram(int progId, String resultPath) throws Exception {

			switch(progId) {
			case 1: {
				/*
				 * Test aggregator without parameter for iterate
				 */

				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setDegreeOfParallelism(DOP);

				DataSet<Integer> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env);
				IterativeDataSet<Integer> iteration = initialSolutionSet.iterate(MAX_ITERATIONS);

				// register aggregator
				LongSumAggregator aggr = new LongSumAggregator();
				iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);
				
				// register convergence criterion
				iteration.registerAggregationConvergenceCriterion(NEGATIVE_ELEMENTS_AGGR, aggr, 
						new NegativeElementsConvergenceCriterion());
				
				DataSet<Integer> updatedDs = iteration.map(new SubtractOneMap());
				iteration.closeWith(updatedDs).writeAsText(resultPath);
				env.execute();

				// return expected result
				return "-3\n" + "-2\n" + "-2\n" + "-1\n" + "-1\n"
				 		+ "-1\n" + "0\n" + "0\n" + "0\n" + "0\n"
				 		+ "1\n" + "1\n" + "1\n" + "1\n" + "1\n";
			}
			case 2: {
				/*
				 * Test aggregator with parameter for iterate
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setDegreeOfParallelism(DOP);

				DataSet<Integer> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env);
				IterativeDataSet<Integer> iteration = initialSolutionSet.iterate(MAX_ITERATIONS);

				// register aggregator
				LongSumAggregatorWithParameter aggr = new LongSumAggregatorWithParameter(0);
				iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);
				
				// register convergence criterion
				iteration.registerAggregationConvergenceCriterion(NEGATIVE_ELEMENTS_AGGR, aggr, 
						new NegativeElementsConvergenceCriterion());
				
				DataSet<Integer> updatedDs = iteration.map(new SubtractOneMapWithParam());
				iteration.closeWith(updatedDs).writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "-3\n" + "-2\n" + "-2\n" + "-1\n" + "-1\n"
				 		+ "-1\n" + "0\n" + "0\n" + "0\n" + "0\n"
				 		+ "1\n" + "1\n" + "1\n" + "1\n" + "1\n";
			}
			case 3: {
				/*
				 * Test convergence criterion with parameter for iterate
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setDegreeOfParallelism(DOP);

				DataSet<Integer> initialSolutionSet = CollectionDataSets.getIntegerDataSet(env);
				IterativeDataSet<Integer> iteration = initialSolutionSet.iterate(MAX_ITERATIONS);

				// register aggregator
				LongSumAggregator aggr = new LongSumAggregator();
				iteration.registerAggregator(NEGATIVE_ELEMENTS_AGGR, aggr);
				
				// register convergence criterion
				iteration.registerAggregationConvergenceCriterion(NEGATIVE_ELEMENTS_AGGR, aggr, 
						new NegativeElementsConvergenceCriterionWithParam(3));
				
				DataSet<Integer> updatedDs = iteration.map(new SubtractOneMap());
				iteration.closeWith(updatedDs).writeAsText(resultPath);
				env.execute();
				
				// return expected result
				return "-3\n" + "-2\n" + "-2\n" + "-1\n" + "-1\n"
				 		+ "-1\n" + "0\n" + "0\n" + "0\n" + "0\n"
				 		+ "1\n" + "1\n" + "1\n" + "1\n" + "1\n";
			}
			case 4: {
				/*
				 * Test aggregator without parameter for iterateDelta
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setDegreeOfParallelism(DOP);
				
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
				DataSet<Integer> result = iterationRes.map(new ProjectSecondMapper());
				result.writeAsText(resultPath);
				
				env.execute();
				
				// return expected result
				return "1\n" + "2\n" + "2\n" + "3\n" + "3\n"
				 		+ "3\n" + "4\n" + "4\n" + "4\n" + "4\n"
				 		+ "5\n" + "5\n" + "5\n" + "5\n" + "5\n";
				
			}
			case 5: {
				/*
				 * Test aggregator with parameter for iterateDelta
				 */
				
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setDegreeOfParallelism(DOP);
				
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
				DataSet<Integer> result = iterationRes.map(new ProjectSecondMapper());
				result.writeAsText(resultPath);
				
				env.execute();
				
				// return expected result
				return "1\n" + "2\n" + "2\n" + "3\n" + "3\n"
				 		+ "3\n" + "4\n" + "4\n" + "4\n" + "4\n"
				 		+ "5\n" + "5\n" + "5\n" + "5\n" + "5\n";
			}
			default:
				throw new IllegalArgumentException("Invalid program id");
			}

		}
	}

	@SuppressWarnings("serial")
	public static final class NegativeElementsConvergenceCriterion implements ConvergenceCriterion<LongValue> {

		@Override
		public boolean isConverged(int iteration, LongValue value) {
			return value.getValue() > 3;
		}
	}

	@SuppressWarnings("serial")
	public static final class NegativeElementsConvergenceCriterionWithParam implements ConvergenceCriterion<LongValue> {

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
	public static final class SubtractOneMap extends MapFunction<Integer, Integer> {

		private LongSumAggregator aggr;

		@Override
		public void open(Configuration conf) {

			aggr = getIterationRuntimeContext().getIterationAggregator(AggregatorProgs.NEGATIVE_ELEMENTS_AGGR);
		}

		@Override
		public Integer map(Integer value) {
			Integer newValue = new Integer(value.intValue() - 1);
			// count negative numbers
			if (newValue.intValue() < 0) {
				aggr.aggregate(1l);
			}
			return newValue;
		}
	}

	@SuppressWarnings("serial")
	public static final class SubtractOneMapWithParam extends MapFunction<Integer, Integer> {

		private LongSumAggregatorWithParameter aggr;

		@Override
		public void open(Configuration conf) {

			aggr = getIterationRuntimeContext().getIterationAggregator(AggregatorProgs.NEGATIVE_ELEMENTS_AGGR);
		}

		@Override
		public Integer map(Integer value) {
			Integer newValue = new Integer(value.intValue() - 1);
			// count numbers less then the aggregator parameter
			if ( newValue.intValue() < aggr.getValue() ) {
				aggr.aggregate(1l);
			}
			return newValue;
		}
	}

	@SuppressWarnings("serial")
	public static class LongSumAggregatorWithParameter extends LongSumAggregator {

		private int value;

		public LongSumAggregatorWithParameter(int val) {
			this.value = val;
		}

		public int getValue() {
			return this.value;
		}
	}

	@SuppressWarnings("serial")
	public static final class TupleMakerMap extends MapFunction<Integer, Tuple2<Integer, Integer>> {

		@Override
		public Tuple2<Integer, Integer> map(Integer value) throws Exception {
			Random ran = new Random();
			Integer nodeId = new Integer(ran.nextInt(100000));
			return new Tuple2<Integer, Integer>(nodeId, value);
		}

	}

	@SuppressWarnings("serial")
	public static final class AggregateMapDelta extends MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		private LongSumAggregator aggr;
		private LongValue previousAggr;
		private int superstep;

		@Override
		public void open(Configuration conf) {

			aggr = getIterationRuntimeContext().getIterationAggregator(AggregatorProgs.NEGATIVE_ELEMENTS_AGGR);
			superstep = getIterationRuntimeContext().getSuperstepNumber();

			if (superstep > 1) {
				previousAggr = getIterationRuntimeContext().getPreviousIterationAggregate(AggregatorProgs.NEGATIVE_ELEMENTS_AGGR);
				// check previous aggregator value
				Assert.assertEquals(superstep - 1, previousAggr.getValue());
			}

		}

		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) {
			// count the elements that are equal to the superstep number
			if (value.f1.intValue() == superstep) {
				aggr.aggregate(1l);
			}
			return value;
		}
	}

	@SuppressWarnings("serial")
	public static final class UpdateFilter extends FlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, 
		Tuple2<Integer, Integer>> {

		private int superstep;

		@Override
		public void open(Configuration conf) { 

			superstep = getIterationRuntimeContext().getSuperstepNumber();

		}

		@Override
		public void flatMap(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> value,
				Collector<Tuple2<Integer, Integer>> out) throws Exception {

			if (value.f0.f1  > superstep) {
				out.collect(value.f0);
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class ProjectSecondMapper extends MapFunction<Tuple2<Integer, Integer>, Integer> {

		@Override
		public Integer map(Tuple2<Integer, Integer> value) {
			return value.f1;
		}
	}

	@SuppressWarnings("serial")
	public static final class AggregateMapDeltaWithParam extends MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		private LongSumAggregatorWithParameter aggr;
		private LongValue previousAggr;
		private int superstep;

		@Override
		public void open(Configuration conf) {

			aggr = getIterationRuntimeContext().getIterationAggregator(AggregatorProgs.NEGATIVE_ELEMENTS_AGGR);
			superstep = getIterationRuntimeContext().getSuperstepNumber();

			if (superstep > 1) {
				previousAggr = getIterationRuntimeContext().getPreviousIterationAggregate(AggregatorProgs.NEGATIVE_ELEMENTS_AGGR);

				// check previous aggregator value
				switch(superstep) {
					case 2: {
						Assert.assertEquals(6, previousAggr.getValue());
					}
					case 3: {
						Assert.assertEquals(5, previousAggr.getValue());
					}
					case 4: {
						Assert.assertEquals(3, previousAggr.getValue());
					}
					case 5: {
						Assert.assertEquals(0, previousAggr.getValue());
					}
					default:
				}
				Assert.assertEquals(superstep-1, previousAggr.getValue());
			}

		}

		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) {
			// count the elements that are equal to the superstep number
			if (value.f1.intValue() < aggr.getValue()) {
				aggr.aggregate(1l);
			}
			return value;
		}
	}

}
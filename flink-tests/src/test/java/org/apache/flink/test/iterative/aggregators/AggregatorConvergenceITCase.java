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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 * Connected Components test case that uses a parameterizable convergence criterion
 */
@RunWith(Parameterized.class)
@SuppressWarnings("serial")
public class AggregatorConvergenceITCase extends MultipleProgramsTestBase {

	public AggregatorConvergenceITCase(TestExecutionMode mode) {
		super(mode);
	}
	
	@Test
	public void testConnectedComponentsWithParametrizableConvergence() {
		try {
			List<Tuple2<Long, Long>> verticesInput = Arrays.asList(
					new Tuple2<Long, Long>(1l,1l),
					new Tuple2<Long, Long>(2l,2l),
					new Tuple2<Long, Long>(3l,3l),
					new Tuple2<Long, Long>(4l,4l),
					new Tuple2<Long, Long>(5l,5l),
					new Tuple2<Long, Long>(6l,6l),
					new Tuple2<Long, Long>(7l,7l),
					new Tuple2<Long, Long>(8l,8l),
					new Tuple2<Long, Long>(9l,9l)
			);
			
			List<Tuple2<Long, Long>> edgesInput = Arrays.asList(
					new Tuple2<Long, Long>(1l,2l),
					new Tuple2<Long, Long>(1l,3l),
					new Tuple2<Long, Long>(2l,3l),
					new Tuple2<Long, Long>(2l,4l),
					new Tuple2<Long, Long>(2l,1l),
					new Tuple2<Long, Long>(3l,1l),
					new Tuple2<Long, Long>(3l,2l),
					new Tuple2<Long, Long>(4l,2l),
					new Tuple2<Long, Long>(4l,6l),
					new Tuple2<Long, Long>(5l,6l),
					new Tuple2<Long, Long>(6l,4l),
					new Tuple2<Long, Long>(6l,5l),
					new Tuple2<Long, Long>(7l,8l),
					new Tuple2<Long, Long>(7l,9l),
					new Tuple2<Long, Long>(8l,7l),
					new Tuple2<Long, Long>(8l,9l),
					new Tuple2<Long, Long>(9l,7l),
					new Tuple2<Long, Long>(9l,8l)
			);

			// name of the aggregator that checks for convergence
			final String UPDATED_ELEMENTS = "updated.elements.aggr";

			// the iteration stops if less than this number os elements change value
			final long convergence_threshold = 3;

			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> initialSolutionSet = env.fromCollection(verticesInput);
			DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgesInput);

			IterativeDataSet<Tuple2<Long, Long>> iteration =
					initialSolutionSet.iterate(10);

			// register the convergence criterion
			iteration.registerAggregationConvergenceCriterion(UPDATED_ELEMENTS,
					new LongSumAggregator(), new UpdatedElementsConvergenceCriterion(convergence_threshold));

			DataSet<Tuple2<Long, Long>> verticesWithNewComponents = iteration.join(edges).where(0).equalTo(0)
					.with(new NeighborWithComponentIDJoin())
					.groupBy(0).min(1);

			DataSet<Tuple2<Long, Long>> updatedComponentId =
					verticesWithNewComponents.join(iteration).where(0).equalTo(0)
							.flatMap(new MinimumIdFilter(UPDATED_ELEMENTS));

			List<Tuple2<Long, Long>> result = iteration.closeWith(updatedComponentId).collect();
			Collections.sort(result, new JavaProgramTestBase.TupleComparator<Tuple2<Long, Long>>());

			List<Tuple2<Long, Long>> expectedResult = Arrays.asList(
					new Tuple2<Long, Long>(1L,1L),
					new Tuple2<Long, Long>(2L,1L),
					new Tuple2<Long, Long>(3L,1L),
					new Tuple2<Long, Long>(4L,1L),
					new Tuple2<Long, Long>(5L,2L),
					new Tuple2<Long, Long>(6L,1L),
					new Tuple2<Long, Long>(7L,7L),
					new Tuple2<Long, Long>(8L,7L),
					new Tuple2<Long, Long>(9L,7L)
			);
			
			assertEquals(expectedResult, result);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testParameterizableAggregator() {
		try {
			List<Tuple2<Long, Long>> verticesInput = Arrays.asList(
				new Tuple2<Long, Long>(1l,1l),
				new Tuple2<Long, Long>(2l,2l),
				new Tuple2<Long, Long>(3l,3l),
				new Tuple2<Long, Long>(4l,4l),
				new Tuple2<Long, Long>(5l,5l),
				new Tuple2<Long, Long>(6l,6l),
				new Tuple2<Long, Long>(7l,7l),
				new Tuple2<Long, Long>(8l,8l),
				new Tuple2<Long, Long>(9l,9l)
			);
			
			List<Tuple2<Long, Long>> edgesInput = Arrays.asList(
					new Tuple2<>(1l,2l),
					new Tuple2<>(1l,3l),
					new Tuple2<>(2l,3l),
					new Tuple2<>(2l,4l),
					new Tuple2<>(2l,1l),
					new Tuple2<>(3l,1l),
					new Tuple2<>(3l,2l),
					new Tuple2<>(4l,2l),
					new Tuple2<>(4l,6l),
					new Tuple2<>(5l,6l),
					new Tuple2<>(6l,4l),
					new Tuple2<>(6l,5l),
					new Tuple2<>(7l,8l),
					new Tuple2<>(7l,9l),
					new Tuple2<>(8l,7l),
					new Tuple2<>(8l,9l),
					new Tuple2<>(9l,7l),
					new Tuple2<>(9l,8l)
			);

			final int MAX_ITERATIONS = 5;
			final String AGGREGATOR_NAME = "elements.in.component.aggregator";
			final long componentId = 1l;

			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> initialSolutionSet = env.fromCollection(verticesInput);
			DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgesInput);

			IterativeDataSet<Tuple2<Long, Long>> iteration =
					initialSolutionSet.iterate(MAX_ITERATIONS);

			// register the aggregator
			iteration.registerAggregator(AGGREGATOR_NAME, new LongSumAggregatorWithParameter(componentId));

			DataSet<Tuple2<Long, Long>> verticesWithNewComponents = iteration.join(edges).where(0).equalTo(0)
					.with(new NeighborWithComponentIDJoin())
					.groupBy(0).min(1);

			DataSet<Tuple2<Long, Long>> updatedComponentId =
					verticesWithNewComponents.join(iteration).where(0).equalTo(0)
							.flatMap(new MinimumIdFilterCounting(AGGREGATOR_NAME));

			List<Tuple2<Long, Long>> result = iteration.closeWith(updatedComponentId).collect();

			Collections.sort(result, new JavaProgramTestBase.TupleComparator<Tuple2<Long, Long>>());

			List<Tuple2<Long, Long>> expectedResult = Arrays.asList(
					new Tuple2<>(1L,1L),
					new Tuple2<>(2L,1L),
					new Tuple2<>(3L,1L),
					new Tuple2<>(4L,1L),
					new Tuple2<>(5L,1L),
					new Tuple2<>(6L,1L),
					new Tuple2<>(7L,7L),
					new Tuple2<>(8L,7L),
					new Tuple2<>(9L,7L)
			);

			// checkpogram result
			assertEquals(expectedResult, result);

			// check aggregators
			long[] aggr_values = MinimumIdFilterCounting.aggr_value;

			// note that position 0 has the end result from superstep 1, retrieved at the start of iteration 2
			// position one as superstep 2, retrieved at the start of iteration 3.
			// the result from iteration 5 is not available, because no iteration 6 happens
			assertEquals(3, aggr_values[0]);
			assertEquals(4, aggr_values[1]);
			assertEquals(5, aggr_values[2]);
			assertEquals(6, aggr_values[3]);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// ------------------------------------------------------------------------
	//  Test Functions
	// ------------------------------------------------------------------------

	public static final class NeighborWithComponentIDJoin extends RichJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithCompId, Tuple2<Long, Long> edge) {
			vertexWithCompId.f0 = edge.f1;
			return vertexWithCompId;
		}
	}
	
	public static class MinimumIdFilter extends RichFlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		private final String aggName;
		private LongSumAggregator aggr;

		public MinimumIdFilter(String aggName) {
			this.aggName = aggName;
		}

		@Override
		public void open(Configuration conf) {
			aggr = getIterationRuntimeContext().getIterationAggregator(aggName);
		}

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> vertexWithNewAndOldId,
				Collector<Tuple2<Long, Long>> out) {

			if (vertexWithNewAndOldId.f0.f1 < vertexWithNewAndOldId.f1.f1) {
				out.collect(vertexWithNewAndOldId.f0);
				aggr.aggregate(1l);
			}
			else {
				out.collect(vertexWithNewAndOldId.f1);
			}
		}
	}

	public static final class MinimumIdFilterCounting 
			extends RichFlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {
		
		private static final long[] aggr_value = new long[5];
		
		private final String aggName;
		private LongSumAggregatorWithParameter aggr;

		public MinimumIdFilterCounting(String aggName) {
			this.aggName = aggName;
		}
		
		@Override
		public void open(Configuration conf) {
			final int superstep = getIterationRuntimeContext().getSuperstepNumber();
			
			aggr = getIterationRuntimeContext().getIterationAggregator(aggName);
			
			if (superstep > 1 && getIterationRuntimeContext().getIndexOfThisSubtask() == 0) {
				LongValue val = getIterationRuntimeContext().getPreviousIterationAggregate(aggName);
				aggr_value[superstep - 2] = val.getValue();
			}
		}

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> vertexWithNewAndOldId,
				Collector<Tuple2<Long, Long>> out) {

			if (vertexWithNewAndOldId.f0.f1 < vertexWithNewAndOldId.f1.f1) {
				out.collect(vertexWithNewAndOldId.f0);
				if (vertexWithNewAndOldId.f0.f1 == aggr.getComponentId()) {
					aggr.aggregate(1l);
				}
			} else {
				out.collect(vertexWithNewAndOldId.f1);
				if (vertexWithNewAndOldId.f1.f1 == aggr.getComponentId()) {
					aggr.aggregate(1l);
				}
			}
		}
	}

	/** A Convergence Criterion with one parameter */
	public static class UpdatedElementsConvergenceCriterion implements ConvergenceCriterion<LongValue> {

		private final long threshold;

		public UpdatedElementsConvergenceCriterion(long u_threshold) {
			this.threshold = u_threshold;
		}

		@Override
		public boolean isConverged(int iteration, LongValue value) {
			return value.getValue() < this.threshold;
		}
	}

	public static final class LongSumAggregatorWithParameter extends LongSumAggregator {

		private long componentId;

		public LongSumAggregatorWithParameter(long compId) {
			this.componentId = compId;
		}

		public long getComponentId() {
			return this.componentId;
		}
	}
}

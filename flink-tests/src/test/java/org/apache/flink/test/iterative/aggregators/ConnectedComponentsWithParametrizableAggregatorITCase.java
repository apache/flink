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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/**
 * Connected Components test case that uses a parameterizable aggregator
 */
public class ConnectedComponentsWithParametrizableAggregatorITCase extends JavaProgramTestBase {

	private static final int MAX_ITERATIONS = 5;
	private static final int DOP = 1;

	protected static List<Tuple2<Long, Long>> verticesInput = new ArrayList<Tuple2<Long, Long>>();
	protected static List<Tuple2<Long, Long>> edgesInput = new ArrayList<Tuple2<Long, Long>>();
	private String resultPath;
	private String expectedResult;

	@Override
	protected void preSubmit() throws Exception {
		// vertices input
		verticesInput.add(new Tuple2<Long, Long>(1l,1l));
		verticesInput.add(new Tuple2<Long, Long>(2l,2l));
		verticesInput.add(new Tuple2<Long, Long>(3l,3l));
		verticesInput.add(new Tuple2<Long, Long>(4l,4l));
		verticesInput.add(new Tuple2<Long, Long>(5l,5l));
		verticesInput.add(new Tuple2<Long, Long>(6l,6l));
		verticesInput.add(new Tuple2<Long, Long>(7l,7l));
		verticesInput.add(new Tuple2<Long, Long>(8l,8l));
		verticesInput.add(new Tuple2<Long, Long>(9l,9l));

		// vertices input
		edgesInput.add(new Tuple2<Long, Long>(1l,2l));
		edgesInput.add(new Tuple2<Long, Long>(1l,3l));
		edgesInput.add(new Tuple2<Long, Long>(2l,3l));
		edgesInput.add(new Tuple2<Long, Long>(2l,4l));
		edgesInput.add(new Tuple2<Long, Long>(2l,1l));
		edgesInput.add(new Tuple2<Long, Long>(3l,1l));
		edgesInput.add(new Tuple2<Long, Long>(3l,2l));
		edgesInput.add(new Tuple2<Long, Long>(4l,2l));
		edgesInput.add(new Tuple2<Long, Long>(4l,6l));
		edgesInput.add(new Tuple2<Long, Long>(5l,6l));
		edgesInput.add(new Tuple2<Long, Long>(6l,4l));
		edgesInput.add(new Tuple2<Long, Long>(6l,5l));
		edgesInput.add(new Tuple2<Long, Long>(7l,8l));
		edgesInput.add(new Tuple2<Long, Long>(7l,9l));
		edgesInput.add(new Tuple2<Long, Long>(8l,7l));
		edgesInput.add(new Tuple2<Long, Long>(8l,9l));
		edgesInput.add(new Tuple2<Long, Long>(9l,7l));
		edgesInput.add(new Tuple2<Long, Long>(9l,8l));

		resultPath = getTempDirPath("result");

		expectedResult = "(1,1)\n" + "(2,1)\n" + "(3,1)\n" + "(4,1)\n" +
						"(5,1)\n" + "(6,1)\n" + "(7,7)\n" + "(8,7)\n" + "(9,7)\n";
	}

	@Override
	protected void testProgram() throws Exception {
		ConnectedComponentsWithAggregatorProgram.runProgram(resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
		long[] aggr_values = ConnectedComponentsWithAggregatorProgram.aggr_value;
		
		// note that position 0 has the end result from superstep 1, retrieved at the start of iteration 2
		// position one as superstep 2, retrieved at the start of iteration 3.
		// the result from iteration 5 is not available, because no iteration 6 happens
		Assert.assertEquals(3, aggr_values[0]);
		Assert.assertEquals(4, aggr_values[1]);
		Assert.assertEquals(5, aggr_values[2]);
		Assert.assertEquals(6, aggr_values[3]);
	}


	private static class ConnectedComponentsWithAggregatorProgram {

		private static final String ELEMENTS_IN_COMPONENT = "elements.in.component.aggregator";
		private static final long componentId = 1l;
		private static long [] aggr_value = new long [MAX_ITERATIONS];

		public static String runProgram(String resultPath) throws Exception {

			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(DOP);

			DataSet<Tuple2<Long, Long>> initialSolutionSet = env.fromCollection(verticesInput);
			DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgesInput);

			IterativeDataSet<Tuple2<Long, Long>> iteration =
					initialSolutionSet.iterate(MAX_ITERATIONS);

			// register the aggregator
			iteration.registerAggregator(ELEMENTS_IN_COMPONENT, new LongSumAggregatorWithParameter(componentId));

			DataSet<Tuple2<Long, Long>> verticesWithNewComponents = iteration.join(edges).where(0).equalTo(0)
					.with(new NeighborWithComponentIDJoin())
					.groupBy(0).reduceGroup(new MinimumReduce());

			DataSet<Tuple2<Long, Long>> updatedComponentId =
					verticesWithNewComponents.join(iteration).where(0).equalTo(0)
					.flatMap(new MinimumIdFilter());

			iteration.closeWith(updatedComponentId).writeAsText(resultPath);

			env.execute();

			return resultPath;
		}
	}

	public static final class NeighborWithComponentIDJoin implements JoinFunction
		<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithCompId,
				Tuple2<Long, Long> edge) throws Exception {

			vertexWithCompId.setField(edge.f1, 0);
			return vertexWithCompId;
		}
	}

	public static final class MinimumReduce extends RichGroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;
		
		private final Tuple2<Long, Long> resultVertex = new Tuple2<Long, Long>();

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) {
			Long vertexId = 0L;
			Long minimumCompId = Long.MAX_VALUE;

			for (Tuple2<Long, Long> value: values) {
				vertexId = value.f0;
				Long candidateCompId = value.f1;
				if (candidateCompId < minimumCompId) {
					minimumCompId = candidateCompId;
				}
			}
			resultVertex.f0 = vertexId;
			resultVertex.f1 = minimumCompId;

			out.collect(resultVertex);
		}
	}

	@SuppressWarnings("serial")
	public static final class MinimumIdFilter extends RichFlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		private static LongSumAggregatorWithParameter aggr;

		@Override
		public void open(Configuration conf) {
			aggr = getIterationRuntimeContext().getIterationAggregator(
					ConnectedComponentsWithAggregatorProgram.ELEMENTS_IN_COMPONENT);

			int superstep = getIterationRuntimeContext().getSuperstepNumber(); 

			if (superstep > 1) {
				LongValue val = getIterationRuntimeContext().getPreviousIterationAggregate(
						ConnectedComponentsWithAggregatorProgram.ELEMENTS_IN_COMPONENT);
				ConnectedComponentsWithAggregatorProgram.aggr_value[superstep-2] = val.getValue();
			}
		}

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> vertexWithNewAndOldId,
				Collector<Tuple2<Long, Long>> out) throws Exception {

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

	// A LongSumAggregator with one parameter
	@SuppressWarnings("serial")
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

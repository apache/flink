/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.iterative.aggregators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import eu.stratosphere.api.common.aggregators.LongSumAggregator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.JavaProgramTestBase;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.util.Collector;


/**
 * 
 * Connected Components test case that uses a parametrizable aggregator
 *
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

		expectedResult = "(1, 1)\n" + "(2, 1)\n" + "(3, 1)\n" + "(4, 1)\n" +
						"(5, 1)\n" + "(6, 1)\n" + "(7, 7)\n" + "(8, 7)\n" + "(9, 7)\n";
	}

	@Override
	protected void testProgram() throws Exception {
		ConnectedComponentsWithAggregatorProgram.runProgram(resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
		long[] aggr_values = ConnectedComponentsWithAggregatorProgram.aggr_value;
		Assert.assertEquals(3, aggr_values[0]);
		Assert.assertEquals(4, aggr_values[1]);
		Assert.assertEquals(5, aggr_values[2]);
		Assert.assertEquals(6, aggr_values[3]);
		Assert.assertEquals(6, aggr_values[4]);
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

	public static final class NeighborWithComponentIDJoin extends JoinFunction
		<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithCompId,
				Tuple2<Long, Long> edge) throws Exception {

			vertexWithCompId.setField(edge.f1, 0);
			return vertexWithCompId;
		}
	}

	public static final class MinimumReduce extends GroupReduceFunction
		<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;
		final Tuple2<Long, Long> resultVertex = new Tuple2<Long, Long>();

		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values,
				Collector<Tuple2<Long, Long>> out) throws Exception {

			final Tuple2<Long, Long> first = values.next();
			final Long vertexId = first.f0;
			Long minimumCompId = first.f1;

			while (values.hasNext()) {
				Long candidateCompId = values.next().f1;
				if (candidateCompId < minimumCompId) {
					minimumCompId = candidateCompId;
				}
			}
			resultVertex.setField(vertexId, 0);
			resultVertex.setField(minimumCompId, 1);

			out.collect(resultVertex);
		}
	}

	@SuppressWarnings("serial")
	public static final class MinimumIdFilter extends FlatMapFunction
		<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

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

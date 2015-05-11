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

package org.apache.flink.graph.test;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GSAConfiguration;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.GatherSumApplyIteration;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.LongValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class GatherSumApplyConfigurationITCase extends MultipleProgramsTestBase {

	public GatherSumApplyConfigurationITCase(TestExecutionMode mode) {
		super(mode);
	}

	private String resultPath;
	private String expectedResult;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testRunWithConfiguration() throws Exception {
		/*
		 * Test Graph's runGatherSumApplyIteration when configuration parameters are provided
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromCollection(TestGraphUtils.getLongLongVertices(),
				TestGraphUtils.getLongLongEdges(), env).mapVertices(new AssignOneMapper());

		// create the configuration object
		GSAConfiguration parameters = new GSAConfiguration();

		parameters.addBroadcastSetForGatherFunction("gatherBcastSet", env.fromElements(1, 2, 3));
		parameters.addBroadcastSetForSumFunction("sumBcastSet", env.fromElements(4, 5, 6));
		parameters.addBroadcastSetForApplyFunction("applyBcastSet", env.fromElements(7, 8, 9));
		parameters.registerAggregator("superstepAggregator", new LongSumAggregator());

		Graph<Long, Long, Long> result = graph.runGatherSumApplyIteration(new Gather(), new Sum(),
				new Apply(), 10, parameters);

		result.getVertices().writeAsCsv(resultPath, "\n", "\t");
		env.execute();

		expectedResult = "1	11\n" +
				"2	11\n" +
				"3	11\n" +
				"4	11\n" +
				"5	11";
	}

	@Test
	public void testIterationConfiguration() throws Exception {

		/*
		 * Test name, parallelism and solutionSetUnmanaged parameters
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		GatherSumApplyIteration<Long, Long, Long, Long> iteration = GatherSumApplyIteration
				.withEdges(TestGraphUtils.getLongLongEdgeData(env), new DummyGather(),
						new DummySum(), new DummyApply(), 10);

		GSAConfiguration parameters = new GSAConfiguration();
		parameters.setName("gelly iteration");
		parameters.setParallelism(2);
		parameters.setSolutionSetUnmanagedMemory(true);

		iteration.configure(parameters);

		Assert.assertEquals("gelly iteration", iteration.getIterationConfiguration().getName(""));
		Assert.assertEquals(2, iteration.getIterationConfiguration().getParallelism());
		Assert.assertEquals(true, iteration.getIterationConfiguration().isSolutionSetUnmanagedMemory());

		DataSet<Vertex<Long, Long>> result = TestGraphUtils.getLongLongVertexData(env).runOperation(iteration);

		result.writeAsCsv(resultPath, "\n", "\t");
		env.execute();
		expectedResult = "1	11\n" +
				"2	12\n" +
				"3	13\n" +
				"4	14\n" +
				"5	15";
	}

	@SuppressWarnings("serial")
	private static final class Gather extends GatherFunction<Long, Long, Long> {

		@Override
		public void preSuperstep() {

			// test bcast variable
			@SuppressWarnings("unchecked")
			List<Tuple1<Integer>> bcastSet = (List<Tuple1<Integer>>)(List<?>)getBroadcastSet("gatherBcastSet");
			Assert.assertEquals(1, bcastSet.get(0));
			Assert.assertEquals(2, bcastSet.get(1));
			Assert.assertEquals(3, bcastSet.get(2));

			// test aggregator
			if (getSuperstepNumber() == 2) {
				long aggrValue = ((LongValue)getPreviousIterationAggregate("superstepAggregator")).getValue();

				Assert.assertEquals(7, aggrValue);
			}
		}

		public Long gather(Neighbor<Long, Long> neighbor) {
			return neighbor.getNeighborValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class Sum extends SumFunction<Long, Long, Long> {

		LongSumAggregator aggregator = new LongSumAggregator();

		@Override
		public void preSuperstep() {

			// test bcast variable
			@SuppressWarnings("unchecked")
			List<Tuple1<Integer>> bcastSet = (List<Tuple1<Integer>>)(List<?>)getBroadcastSet("sumBcastSet");
			Assert.assertEquals(4, bcastSet.get(0));
			Assert.assertEquals(5, bcastSet.get(1));
			Assert.assertEquals(6, bcastSet.get(2));

			// test aggregator
			aggregator = getIterationAggregator("superstepAggregator");
		}

		public Long sum(Long newValue, Long currentValue) {
			long superstep = getSuperstepNumber();
			aggregator.aggregate(superstep);
			return 0l;
		}
	}

	@SuppressWarnings("serial")
	private static final class Apply extends ApplyFunction<Long, Long, Long> {

		LongSumAggregator aggregator = new LongSumAggregator();

		@Override
		public void preSuperstep() {

			// test bcast variable
			@SuppressWarnings("unchecked")
			List<Tuple1<Integer>> bcastSet = (List<Tuple1<Integer>>)(List<?>)getBroadcastSet("applyBcastSet");
			Assert.assertEquals(7, bcastSet.get(0));
			Assert.assertEquals(8, bcastSet.get(1));
			Assert.assertEquals(9, bcastSet.get(2));

			// test aggregator
			aggregator = getIterationAggregator("superstepAggregator");
		}

		public void apply(Long summedValue, Long origValue) {
			long superstep = getSuperstepNumber();
			aggregator.aggregate(superstep);
			setResult(origValue + 1);
		}
	}

	@SuppressWarnings("serial")
	private static final class DummyGather extends GatherFunction<Long, Long, Long> {

		public Long gather(Neighbor<Long, Long> neighbor) {
			return neighbor.getNeighborValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class DummySum extends SumFunction<Long, Long, Long> {

		public Long sum(Long newValue, Long currentValue) {
			return 0l;
		}
	}

	@SuppressWarnings("serial")
	private static final class DummyApply extends ApplyFunction<Long, Long, Long> {

		public void apply(Long summedValue, Long origValue) {
			setResult(origValue + 1);
		}
	}

	@SuppressWarnings("serial")
	public static final class AssignOneMapper implements MapFunction<Vertex<Long, Long>, Long> {

		public Long map(Vertex<Long, Long> value) {
			return 1l;
		}
	}
}

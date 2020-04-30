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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Iterative Connected Components test case which recomputes only the elements
 * of the solution set whose at least one dependency (in-neighbor) has changed since the last iteration.
 * Requires two joins with the solution set.
 */
@SuppressWarnings("serial")
public class DependencyConnectedComponentsITCase extends JavaProgramTestBase {

	private static final int MAX_ITERATIONS = 20;
	private static final int parallelism = 1;

	protected static List<Tuple2<Long, Long>> verticesInput = new ArrayList<Tuple2<Long, Long>>();
	protected static List<Tuple2<Long, Long>> edgesInput = new ArrayList<Tuple2<Long, Long>>();
	private String resultPath;
	private String expectedResult;

	@Override
	protected void preSubmit() throws Exception {
		verticesInput.clear();
		edgesInput.clear();

		// vertices input
		verticesInput.add(new Tuple2<>(1L, 1L));
		verticesInput.add(new Tuple2<>(2L, 2L));
		verticesInput.add(new Tuple2<>(3L, 3L));
		verticesInput.add(new Tuple2<>(4L, 4L));
		verticesInput.add(new Tuple2<>(5L, 5L));
		verticesInput.add(new Tuple2<>(6L, 6L));
		verticesInput.add(new Tuple2<>(7L, 7L));
		verticesInput.add(new Tuple2<>(8L, 8L));
		verticesInput.add(new Tuple2<>(9L, 9L));

		// vertices input
		edgesInput.add(new Tuple2<>(1L, 2L));
		edgesInput.add(new Tuple2<>(1L, 3L));
		edgesInput.add(new Tuple2<>(2L, 3L));
		edgesInput.add(new Tuple2<>(2L, 4L));
		edgesInput.add(new Tuple2<>(2L, 1L));
		edgesInput.add(new Tuple2<>(3L, 1L));
		edgesInput.add(new Tuple2<>(3L, 2L));
		edgesInput.add(new Tuple2<>(4L, 2L));
		edgesInput.add(new Tuple2<>(4L, 6L));
		edgesInput.add(new Tuple2<>(5L, 6L));
		edgesInput.add(new Tuple2<>(6L, 4L));
		edgesInput.add(new Tuple2<>(6L, 5L));
		edgesInput.add(new Tuple2<>(7L, 8L));
		edgesInput.add(new Tuple2<>(7L, 9L));
		edgesInput.add(new Tuple2<>(8L, 7L));
		edgesInput.add(new Tuple2<>(8L, 9L));
		edgesInput.add(new Tuple2<>(9L, 7L));
		edgesInput.add(new Tuple2<>(9L, 8L));

		resultPath = getTempDirPath("result");

		expectedResult = "(1,1)\n" + "(2,1)\n" + "(3,1)\n" + "(4,1)\n" +
						"(5,1)\n" + "(6,1)\n" + "(7,7)\n" + "(8,7)\n" + "(9,7)\n";
	}

	@Override
	protected void testProgram() throws Exception {
		DependencyConnectedComponentsProgram.runProgram(resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	private static class DependencyConnectedComponentsProgram {

		public static String runProgram(String resultPath) throws Exception {

			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(parallelism);

			DataSet<Tuple2<Long, Long>> initialSolutionSet = env.fromCollection(verticesInput);
			DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgesInput);
			int keyPosition = 0;

			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
					initialSolutionSet.iterateDelta(initialSolutionSet, MAX_ITERATIONS, keyPosition);

			DataSet<Long> candidates = iteration.getWorkset().join(edges).where(0).equalTo(0)
					.with(new FindCandidatesJoin())
					.groupBy(new KeySelector<Long, Long>() {
						public Long getKey(Long id) {
							return id;
						}
					}).reduceGroup(new RemoveDuplicatesReduce());

			DataSet<Tuple2<Long, Long>> candidatesDependencies =
					candidates.join(edges)
					.where(new KeySelector<Long, Long>() {
						public Long getKey(Long id) {
							return id;
						}
					}).equalTo(new KeySelector<Tuple2<Long, Long>, Long>() {
						public Long getKey(Tuple2<Long, Long> vertexWithId) {
							return vertexWithId.f1;
						}
					}).with(new FindCandidatesDependenciesJoin());

			DataSet<Tuple2<Long, Long>> verticesWithNewComponents =
					candidatesDependencies.join(iteration.getSolutionSet()).where(0).equalTo(0)
					.with(new NeighborWithComponentIDJoin())
					.groupBy(0).reduceGroup(new MinimumReduce());

			DataSet<Tuple2<Long, Long>> updatedComponentId =
					verticesWithNewComponents.join(iteration.getSolutionSet()).where(0).equalTo(0)
					.flatMap(new MinimumIdFilter());

			iteration.closeWith(updatedComponentId, updatedComponentId).writeAsText(resultPath);

			env.execute();

			return resultPath;
		}
	}

	private static final class FindCandidatesJoin extends RichJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long join(Tuple2<Long, Long> vertexWithCompId,
				Tuple2<Long, Long> edge) throws Exception {
			// emit target vertex
			return edge.f1;
		}
	}

	private static final class RemoveDuplicatesReduce extends RichGroupReduceFunction<Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Long> values, Collector<Long> out) {
				out.collect(values.iterator().next());
		}
	}

	private static final class FindCandidatesDependenciesJoin extends RichJoinFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Long> join(Long candidateId, Tuple2<Long, Long> edge) throws Exception {
			return edge;
		}
	}

	private static final class NeighborWithComponentIDJoin extends RichJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> edge,
				Tuple2<Long, Long> vertexWithCompId) throws Exception {

			vertexWithCompId.setField(edge.f1, 0);
			return vertexWithCompId;
		}
	}

	private static final class MinimumReduce extends RichGroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;
		final Tuple2<Long, Long> resultVertex = new Tuple2<Long, Long>();

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

	private static final class MinimumIdFilter extends RichFlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> vertexWithNewAndOldId,
				Collector<Tuple2<Long, Long>> out) {
			if (vertexWithNewAndOldId.f0.f1 < vertexWithNewAndOldId.f1.f1) {
				out.collect(vertexWithNewAndOldId.f0);
			}
		}
	}
}

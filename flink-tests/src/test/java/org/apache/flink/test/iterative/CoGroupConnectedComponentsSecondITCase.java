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

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.java.graph.ConnectedComponents.DuplicateValue;
import org.apache.flink.examples.java.graph.ConnectedComponents.NeighborWithComponentIDJoin;
import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Delta iteration test implementing the connected components algorithm with a cogroup.
 */
@SuppressWarnings("serial")
public class CoGroupConnectedComponentsSecondITCase extends JavaProgramTestBase {

	private static final long SEED = 0xBADC0FFEEBEEFL;

	private static final int NUM_VERTICES = 1000;

	private static final int NUM_EDGES = 10000;

	@Override
	protected void testProgram() throws Exception {

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read vertex and edge data
		DataSet<Long> vertices = env.fromElements(ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES).split("\n"))
				.map(new VertexParser());

		DataSet<Tuple2<Long, Long>> edges = env.fromElements(ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED).split("\n"))
				.flatMap(new EdgeParser());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());

		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
				verticesWithInitialId.iterateDelta(verticesWithInitialId, 100, 0);

		// apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration
				.getWorkset().join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
				.coGroup(iteration.getSolutionSet()).where(0).equalTo(0)
				.with(new MinIdAndUpdate());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);

		// emit result
		List<Tuple2<Long, Long>> resutTuples = new ArrayList<>();
		result.output(new LocalCollectionOutputFormat<>(resutTuples));

		env.execute();
	}

	// --------------------------------------------------------------------------------------------
	//  The test program
	// --------------------------------------------------------------------------------------------

	private static final class VertexParser extends RichMapFunction<String, Long> {

		@Override
		public Long map(String value) throws Exception {
			return Long.parseLong(value);
		}
	}

	private static final class EdgeParser extends RichFlatMapFunction<String, Tuple2<Long, Long>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<Long, Long>> out) throws Exception {
			String[] parts = value.split(" ");
			long v1 = Long.parseLong(parts[0]);
			long v2 = Long.parseLong(parts[1]);

			out.collect(new Tuple2<Long, Long>(v1, v2));
			out.collect(new Tuple2<Long, Long>(v2, v1));
		}
	}

	@ForwardedFieldsFirst("0")
	@ForwardedFieldsSecond("0")
	private static final class MinIdAndUpdate extends RichCoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void coGroup(Iterable<Tuple2<Long, Long>> candidates, Iterable<Tuple2<Long, Long>> current, Collector<Tuple2<Long, Long>> out) {
			Iterator<Tuple2<Long, Long>> iterator = current.iterator();
			if (!iterator.hasNext()) {
				throw new RuntimeException("Error: Id not encountered before.");
			}

			Tuple2<Long, Long> old = iterator.next();

			long minimumComponentID = Long.MAX_VALUE;

			for (Tuple2<Long, Long> candidate : candidates) {
				long candidateComponentID = candidate.f1;
				if (candidateComponentID < minimumComponentID) {
					minimumComponentID = candidateComponentID;
				}
			}

			if (minimumComponentID < old.f1) {
				old.f1 = minimumComponentID;
				out.collect(old);
			}
		}
	}
}

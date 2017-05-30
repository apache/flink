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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.util.Iterator;

/**
 * Delta iteration test implementing the connected components algorithm with a cogroup.
 */
public class CoGroupConnectedComponentsITCase extends JavaProgramTestBase {

	private static final long SEED = 0xBADC0FFEEBEEFL;

	private static final int NUM_VERTICES = 1000;

	private static final int NUM_EDGES = 10000;

	private static final int MAX_ITERATIONS = 100;

	protected String verticesPath;
	protected String edgesPath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			ConnectedComponentsData.checkOddEvenResult(reader);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  The test program
	// --------------------------------------------------------------------------------------------

	@Override
	protected void testProgram() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple1<Long>> initialVertices = env.readCsvFile(verticesPath).fieldDelimiter(" ").types(Long.class).name("Vertices");

		DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(edgesPath).fieldDelimiter(" ").types(Long.class, Long.class).name("Edges");

		DataSet<Tuple2<Long, Long>> verticesWithId = initialVertices.map(new MapFunction<Tuple1<Long>, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Tuple1<Long> value) throws Exception {
				return new Tuple2<>(value.f0, value.f0);
			}
		}).name("Assign Vertex Ids");

		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = verticesWithId.iterateDelta(verticesWithId, MAX_ITERATIONS, 0);

		JoinOperator<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> joinWithNeighbors = iteration.getWorkset()
				.join(edges).where(0).equalTo(0)
				.with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
						return new Tuple2<>(second.f1, first.f1);
					}
				})
				.name("Join Candidate Id With Neighbor");

		CoGroupOperator<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> minAndUpdate = joinWithNeighbors
				.coGroup(iteration.getSolutionSet()).where(0).equalTo(0)
				.with(new MinIdAndUpdate())
				.name("min Id and Update");

		iteration.closeWith(minAndUpdate, minAndUpdate).writeAsCsv(resultPath, "\n", " ").name("Result");

		env.execute("Workset Connected Components");
	}

	@ForwardedFieldsFirst("f1->f1")
	@ForwardedFieldsSecond("f0->f0")
	private static final class MinIdAndUpdate implements CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterable<Tuple2<Long, Long>> first, Iterable<Tuple2<Long, Long>> second, Collector<Tuple2<Long, Long>> out) throws Exception {
			Iterator<Tuple2<Long, Long>> current = second.iterator();
			if (!current.hasNext()) {
				throw new Exception("Error: Id not encountered before.");
			}
			Tuple2<Long, Long> old = current.next();
			long oldId = old.f1;

			long minimumComponentID = Long.MAX_VALUE;

			for (Tuple2<Long, Long> candidate : first) {
				long candidateComponentID = candidate.f1;
				if (candidateComponentID < minimumComponentID) {
					minimumComponentID = candidateComponentID;
				}
			}

			if (minimumComponentID < oldId) {
				out.collect(new Tuple2<>(old.f0, minimumComponentID));
			}
		}
	}
}

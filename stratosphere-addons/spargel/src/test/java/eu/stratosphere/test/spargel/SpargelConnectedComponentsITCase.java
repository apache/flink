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

package eu.stratosphere.test.spargel;

import java.io.BufferedReader;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.spargel.java.VertexCentricIteration;
import eu.stratosphere.spargel.java.examples.SpargelConnectedComponents.CCMessager;
import eu.stratosphere.spargel.java.examples.SpargelConnectedComponents.CCUpdater;
import eu.stratosphere.spargel.java.examples.SpargelConnectedComponents.IdAssigner;
import eu.stratosphere.test.testdata.ConnectedComponentsData;
import eu.stratosphere.test.util.JavaProgramTestBase;

@SuppressWarnings("serial")
public class SpargelConnectedComponentsITCase extends JavaProgramTestBase {

	private static final long SEED = 9487520347802987L;
	
	private static final int NUM_VERTICES = 1000;
	
	private static final int NUM_EDGES = 10000;

	private String resultPath;
	
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Long> vertexIds = env.generateSequence(1, NUM_VERTICES);
		DataSet<String> edgeString = env.fromElements(ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED).split("\n"));
		
		DataSet<Tuple2<Long, Long>> edges = edgeString.map(new EdgeParser());
		
		DataSet<Tuple2<Long, Long>> initialVertices = vertexIds.map(new IdAssigner());
		DataSet<Tuple2<Long, Long>> result = initialVertices.runOperation(VertexCentricIteration.withPlainEdges(edges, new CCUpdater(), new CCMessager(), 100));
		
		result.print();
		env.execute("Spargel Connected Components");
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			ConnectedComponentsData.checkOddEvenResult(reader);
		}
	}
	
	public static final class EdgeParser extends MapFunction<String, Tuple2<Long, Long>> {
		public Tuple2<Long, Long> map(String value) {
			String[] nums = value.split(" ");
			return new Tuple2<Long, Long>(Long.parseLong(nums[0]), Long.parseLong(nums[1]));
		}
	}
}
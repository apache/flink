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

package org.apache.flink.graph.test.operations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomType;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphCreationWithMapperITCase extends MultipleProgramsTestBase {

	public GraphCreationWithMapperITCase(TestExecutionMode mode){
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
	public void testWithDoubleValueMapper() throws Exception {
		/*
		 * Test create() with edge dataset and a mapper that assigns a double constant as value
	     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Double, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongEdgeData(env),
				new AssignDoubleValueMapper(), env);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,0.1\n" +
				"2,0.1\n" +
				"3,0.1\n" +
				"4,0.1\n" +
				"5,0.1\n";
	}

	@Test
	public void testWithTuple2ValueMapper() throws Exception {
		/*
		 * Test create() with edge dataset and a mapper that assigns a Tuple2 as value
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Tuple2<Long, Long>, Long> graph = Graph.fromDataSet(
				TestGraphUtils.getLongLongEdgeData(env), new AssignTuple2ValueMapper(), env);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,(2,42)\n" +
				"2,(4,42)\n" +
				"3,(6,42)\n" +
				"4,(8,42)\n" +
				"5,(10,42)\n";
	}

	@Test
	public void testWithConstantValueMapper() throws Exception {
	/*
	 * Test create() with edge dataset with String key type
	 * and a mapper that assigns a double constant as value
	 */
	final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	Graph<String, Double, Long> graph = Graph.fromDataSet(TestGraphUtils.getStringLongEdgeData(env),
			new AssignDoubleConstantMapper(), env);

	graph.getVertices().writeAsCsv(resultPath);
	env.execute();
	expectedResult = "1,0.1\n" +
			"2,0.1\n" +
			"3,0.1\n" +
			"4,0.1\n" +
			"5,0.1\n";
	}

	@Test
	public void testWithDCustomValueMapper() throws Exception {
		/*
		 * Test create() with edge dataset and a mapper that assigns a custom vertex value
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, DummyCustomType, Long> graph = Graph.fromDataSet(
				TestGraphUtils.getLongLongEdgeData(env), new AssignCustomValueMapper(), env);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,(F,0)\n" +
				"2,(F,1)\n" +
				"3,(F,2)\n" +
				"4,(F,3)\n" +
				"5,(F,4)\n";
	}

	@SuppressWarnings("serial")
	private static final class AssignDoubleValueMapper implements MapFunction<Long, Double> {
		public Double map(Long value) {
			return 0.1d;
		}
	}

	@SuppressWarnings("serial")
	private static final class AssignTuple2ValueMapper implements MapFunction<Long, Tuple2<Long, Long>> {
		public Tuple2<Long, Long> map(Long vertexId) {
			return new Tuple2<Long, Long>(vertexId*2, 42l);
		}
	}

	@SuppressWarnings("serial")
	private static final class AssignDoubleConstantMapper implements MapFunction<String, Double> {
		public Double map(String value) {
			return 0.1d;
		}
	}

	@SuppressWarnings("serial")
	private static final class AssignCustomValueMapper implements MapFunction<Long, DummyCustomType> {
		public DummyCustomType map(Long vertexId) {
			return new DummyCustomType(vertexId.intValue()-1, false);
		}
	}
}

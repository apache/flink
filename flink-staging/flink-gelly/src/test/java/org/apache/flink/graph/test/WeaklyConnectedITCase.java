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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class WeaklyConnectedITCase extends MultipleProgramsTestBase {

	public WeaklyConnectedITCase(TestExecutionMode mode){
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
	public void testWithConnectedDirected() throws Exception {
		/*
		 * Test isWeaklyConnected() with a connected, directed graph
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		env.fromElements(graph.isWeaklyConnected(10)).writeAsText(resultPath);
		
		env.execute();
		expectedResult = "true\n";
	}

	@Test
	public void testWithDisconnectedDirected() throws Exception {
		/*
		 * Test isWeaklyConnected() with a disconnected, directed graph
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getDisconnectedLongLongEdgeData(env), env);
		
		env.fromElements(graph.isWeaklyConnected(10)).writeAsText(resultPath);
		
		env.execute();
		expectedResult = "false\n";
	}

	@Test
	public void testWithConnectedUndirected() throws Exception {
		/*
		 * Test isWeaklyConnected() with a connected, undirected graph
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env).getUndirected();
		
		env.fromElements(graph.isWeaklyConnected(10)).writeAsText(resultPath);
		
		env.execute();
		expectedResult = "true\n";
	}

	@Test
	public void testWithDisconnectedUndirected() throws Exception {
		/*
		 * Test isWeaklyConnected() with a disconnected, undirected graph
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getDisconnectedLongLongEdgeData(env), env).getUndirected();
		
		env.fromElements(graph.isWeaklyConnected(10)).writeAsText(resultPath);
		
		env.execute();
		expectedResult = "false\n";
	}
}

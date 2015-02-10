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
import org.apache.flink.types.NullValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDegrees extends MultipleProgramsTestBase {

	public TestDegrees(MultipleProgramsTestBase.ExecutionMode mode){
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
	public void testOutDegrees() throws Exception {
		/*
		* Test outDegrees()
		*/
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeData(env), env);

        graph.outDegrees().writeAsCsv(resultPath);
        env.execute();

        expectedResult = "1,2\n" +
                    "2,1\n" +
                    "3,2\n" +
                    "4,1\n" +
                    "5,1\n";
    }

	@Test
	public void testOutDegreesWithNoOutEdges() throws Exception {
		/*
		 * Test outDegrees() no outgoing edges
		 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeDataWithZeroDegree(env), env);

        graph.outDegrees().writeAsCsv(resultPath);
        env.execute();

        expectedResult = "1,3\n" +
                "2,1\n" +
                "3,1\n" +
                "4,1\n" +
                "5,0\n";
    }

	@Test
	public void testInDegrees() throws Exception {
		/*
		 * Test inDegrees()
		 */
	    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	    Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
	            TestGraphUtils.getLongLongEdgeData(env), env);

	    graph.inDegrees().writeAsCsv(resultPath);
	    env.execute();
	    expectedResult = "1,1\n" +
		            "2,1\n" +
		            "3,2\n" +
		            "4,1\n" +
		            "5,2\n";
    }

	@Test
	public void testInDegreesWithNoInEdge() throws Exception {
		/*
		 * Test inDegrees() no ingoing edge
		 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeDataWithZeroDegree(env), env);

        graph.inDegrees().writeAsCsv(resultPath);
        env.execute();
        expectedResult = "1,0\n" +
	                "2,1\n" +
	                "3,1\n" +
	                "4,1\n" +
	                "5,3\n";
    }

	@Test
	public void testGetDegrees() throws Exception {
		/*
		 * Test getDegrees()
		 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
                TestGraphUtils.getLongLongEdgeData(env), env);

        graph.getDegrees().writeAsCsv(resultPath);
        env.execute();
        expectedResult = "1,3\n" +
	                "2,2\n" +
	                "3,4\n" +
	                "4,2\n" +
	                "5,3\n";
    }

	@Test
	public void testGetDegreesWithDisconnectedData() throws Exception {
        /*
		 * Test getDegrees() with disconnected data
		 */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, NullValue, Long> graph =
                Graph.fromDataSet(TestGraphUtils.getDisconnectedLongLongEdgeData(env), env);

        graph.outDegrees().writeAsCsv(resultPath);
        env.execute();
        expectedResult = "1,2\n" +
                "2,1\n" +
                "3,0\n" +
                "4,1\n" +
                "5,0\n";
    }
}
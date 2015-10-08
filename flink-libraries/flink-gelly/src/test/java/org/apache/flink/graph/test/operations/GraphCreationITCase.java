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

import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomParameterizedType;
import org.apache.flink.graph.validation.InvalidVertexIdsValidator;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphCreationITCase extends MultipleProgramsTestBase {

	public GraphCreationITCase(TestExecutionMode mode){
		super(mode);
	}


    private String expectedResult;

	@Test
	public void testCreateWithoutVertexValues() throws Exception {
	/*
	 * Test create() with edge dataset and no vertex values
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, NullValue, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongEdgeData(env), env);

        DataSet<Vertex<Long,NullValue>> data = graph.getVertices();
        List<Vertex<Long,NullValue>> result= data.collect();
        
		expectedResult = "1,(null)\n" +
					"2,(null)\n" +
					"3,(null)\n" +
					"4,(null)\n" +
					"5,(null)\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testCreateWithMapper() throws Exception {
	/*
	 * Test create() with edge dataset and a mapper that assigns the id as value
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongEdgeData(env),
				new AssignIdAsValueMapper(), env);

        DataSet<Vertex<Long,Long>> data = graph.getVertices();
        List<Vertex<Long,Long>> result= data.collect();
        
		expectedResult = "1,1\n" +
					"2,2\n" +
					"3,3\n" +
					"4,4\n" +
					"5,5\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testCreateWithCustomVertexValue() throws Exception {
		/*
		 * Test create() with edge dataset and a mapper that assigns a parametrized custom vertex value
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, DummyCustomParameterizedType<Double>, Long> graph = Graph.fromDataSet(
				TestGraphUtils.getLongLongEdgeData(env), new AssignCustomVertexValueMapper(), env);

        DataSet<Vertex<Long,DummyCustomParameterizedType<Double>>> data = graph.getVertices();
        List<Vertex<Long,DummyCustomParameterizedType<Double>>> result= data.collect();
        
		expectedResult = "1,(2.0,0)\n" +
				"2,(4.0,1)\n" +
				"3,(6.0,2)\n" +
				"4,(8.0,3)\n" +
				"5,(10.0,4)\n";
		
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testValidate() throws Exception {
		/*
		 * Test validate():
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Vertex<Long, Long>> vertices = TestGraphUtils.getLongLongVertexData(env);
		DataSet<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdgeData(env);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(vertices, edges, env);
		Boolean valid = graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>());

		//env.fromElements(result).writeAsText(resultPath);
		
		String res= valid.toString();//env.fromElements(valid);
        List<String> result= new LinkedList<String>();
        result.add(res);
		expectedResult = "true";
		
		compareResultAsText(result, expectedResult);
	}

	@Test
	public void testValidateWithInvalidIds() throws Exception {
		/*
		 * Test validate() - invalid vertex ids
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Vertex<Long, Long>> vertices = TestGraphUtils.getLongLongInvalidVertexData(env);
		DataSet<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdgeData(env);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(vertices, edges, env);
		Boolean valid = graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>());
		
		String res= valid.toString();//env.fromElements(valid);
        List<String> result= new LinkedList<String>();
        result.add(res);

		expectedResult = "false\n";
		
		compareResultAsText(result, expectedResult);
	}

	@SuppressWarnings("serial")
	private static final class AssignIdAsValueMapper implements MapFunction<Long, Long> {
		public Long map(Long vertexId) {
			return vertexId;
		}
	}

	@SuppressWarnings("serial")
	private static final class AssignCustomVertexValueMapper implements
		MapFunction<Long, DummyCustomParameterizedType<Double>> {

		DummyCustomParameterizedType<Double> dummyValue =
				new DummyCustomParameterizedType<Double>();

		public DummyCustomParameterizedType<Double> map(Long vertexId) {
			dummyValue.setIntField(vertexId.intValue()-1);
			dummyValue.setTField(vertexId*2.0);
			return dummyValue;
		}
	}
}
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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomParameterizedType;
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
public class MapVerticesITCase extends MultipleProgramsTestBase {

	public MapVerticesITCase(TestExecutionMode mode){
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
	public void testWithSameValue() throws Exception {
		/*
		 * Test mapVertices() keeping the same value type
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Vertex<Long, Long>> mappedVertices = graph.mapVertices(new AddOneMapper()).getVertices();
		
		mappedVertices.writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,2\n" +
			"2,3\n" +
			"3,4\n" +
			"4,5\n" +
			"5,6\n";
	}

	@Test
	public void testWithStringValue() throws Exception {
		/*
		 * Test mapVertices() and change the value type to String
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Vertex<Long, String>> mappedVertices = graph.mapVertices(new ToStringMapper()).getVertices();
		
		mappedVertices.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,one\n" +
			"2,two\n" +
			"3,three\n" +
			"4,four\n" +
			"5,five\n";
	}

	@Test
	public void testWithtuple1Value() throws Exception {
		/*
		 * Test mapVertices() and change the value type to a Tuple1
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Vertex<Long, Tuple1<Long>>> mappedVertices = graph.mapVertices(new ToTuple1Mapper()).getVertices();
		
		mappedVertices.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,(1)\n" +
			"2,(2)\n" +
			"3,(3)\n" +
			"4,(4)\n" +
			"5,(5)\n";
	}

	@Test
	public void testWithCustomType() throws Exception {
		/*
		 * Test mapVertices() and change the value type to a custom type
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Vertex<Long, DummyCustomType>> mappedVertices = graph.mapVertices(new ToCustomTypeMapper()).getVertices();
		
		mappedVertices.writeAsCsv(resultPath);
		env.execute();

		expectedResult = "1,(T,1)\n" +
			"2,(T,2)\n" +
			"3,(T,3)\n" +
			"4,(T,4)\n" +
			"5,(T,5)\n";
	}

	@Test
	public void testWithCustomParametrizedType() throws Exception {
		/*
		 * Test mapVertices() and change the value type to a parameterized custom type
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
				TestGraphUtils.getLongLongEdgeData(env), env);
		
		DataSet<Vertex<Long, DummyCustomParameterizedType<Double>>> mappedVertices = graph.mapVertices(
				new ToCustomParametrizedTypeMapper()).getVertices();
		
		mappedVertices.writeAsCsv(resultPath);
		env.execute();
	
		expectedResult = "1,(1.0,1)\n" +
			"2,(2.0,2)\n" +
			"3,(3.0,3)\n" +
			"4,(4.0,4)\n" +
			"5,(5.0,5)\n";
	}

	@SuppressWarnings("serial")
	private static final class AddOneMapper implements MapFunction<Vertex<Long, Long>, Long> {
		public Long map(Vertex<Long, Long> value) throws Exception {
			return value.getValue()+1;
		}
	}

	@SuppressWarnings("serial")
	private static final class ToStringMapper implements MapFunction<Vertex<Long, Long>, String> {
		public String map(Vertex<Long, Long> vertex) throws Exception {
			String stringValue;
			if (vertex.getValue() == 1) {
				stringValue = "one";
			}
			else if (vertex.getValue() == 2) {
				stringValue = "two";
			}
			else if (vertex.getValue() == 3) {
				stringValue = "three";
			}
			else if (vertex.getValue() == 4) {
				stringValue = "four";
			}
			else if (vertex.getValue() == 5) {
				stringValue = "five";
			}
			else {
				stringValue = "";
			}
			return stringValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class ToTuple1Mapper implements MapFunction<Vertex<Long, Long>, Tuple1<Long>> {
		public Tuple1<Long> map(Vertex<Long, Long> vertex) throws Exception {
			Tuple1<Long> tupleValue = new Tuple1<Long>();
			tupleValue.setFields(vertex.getValue());
			return tupleValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class ToCustomTypeMapper implements MapFunction<Vertex<Long, Long>, DummyCustomType> {
		public DummyCustomType map(Vertex<Long, Long> vertex) throws Exception {
			DummyCustomType dummyValue = new DummyCustomType();
			dummyValue.setIntField(vertex.getValue().intValue());						
			return dummyValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class ToCustomParametrizedTypeMapper implements MapFunction<Vertex<Long, Long>, 
		DummyCustomParameterizedType<Double>> {
		
		public DummyCustomParameterizedType<Double> map(Vertex<Long, Long> vertex) throws Exception {
			DummyCustomParameterizedType<Double> dummyValue = new DummyCustomParameterizedType<Double>();
			dummyValue.setIntField(vertex.getValue().intValue());
			dummyValue.setTField(new Double(vertex.getValue()));						
			return dummyValue;
		}
	}
}

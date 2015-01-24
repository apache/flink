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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomParameterizedType;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomType;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestMapEdges extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 5;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;
	
	public TestMapEdges(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = GraphProgs.runProgram(curProgId, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}
		
		return toParameterList(tConfigs);
	}
	
	private static class GraphProgs {
		
		@SuppressWarnings("serial")
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Test mapEdges() keeping the same value type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				
				DataSet<Edge<Long, Long>> mappedEdges = graph.mapEdges(new MapFunction<Edge<Long, Long>, Long>() {
					public Long map(Edge<Long, Long> edge) throws Exception {
						return edge.getValue()+1;
					}
				}).getEdges();
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,13\n" +
				"1,3,14\n" +
				"2,3,24\n" +
				"3,4,35\n" +
				"3,5,36\n" + 
				"4,5,46\n" + 
				"5,1,52\n";
			}
			case 2: {
				/*
				 * Test mapEdges() and change the value type to String
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				
				DataSet<Edge<Long, String>> mappedEdges = graph.mapEdges(new MapFunction<Edge<Long, Long>, String>() {
					public String map(Edge<Long, Long> edge) throws Exception {
						return String.format("string(%d)", edge.getValue());
					}
				}).getEdges();
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,string(12)\n" +
				"1,3,string(13)\n" +
				"2,3,string(23)\n" +
				"3,4,string(34)\n" +
				"3,5,string(35)\n" + 
				"4,5,string(45)\n" + 
				"5,1,string(51)\n";
			}
			case 3: {
				/*
				 * Test mapEdges() and change the value type to a Tuple1
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				
				DataSet<Edge<Long, Tuple1<Long>>> mappedEdges = graph.mapEdges(new MapFunction<Edge<Long, Long>, 
						Tuple1<Long>>() {
					public Tuple1<Long> map(Edge<Long, Long> edge) throws Exception {
						Tuple1<Long> tupleValue = new Tuple1<Long>();
						tupleValue.setFields(edge.getValue());
						return tupleValue;
					}
				}).getEdges();
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,(12)\n" +
				"1,3,(13)\n" +
				"2,3,(23)\n" +
				"3,4,(34)\n" +
				"3,5,(35)\n" + 
				"4,5,(45)\n" + 
				"5,1,(51)\n";
			}
			case 4: {
				/*
				 * Test mapEdges() and change the value type to a custom type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				
				DataSet<Edge<Long, DummyCustomType>> mappedEdges = graph.mapEdges(new MapFunction<Edge<Long, Long>, 
						DummyCustomType>() {
					public DummyCustomType map(Edge<Long, Long> edge) throws Exception {
						DummyCustomType dummyValue = new DummyCustomType();
						dummyValue.setIntField(edge.getValue().intValue());						
						return dummyValue;
					}
				}).getEdges();
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,(T,12)\n" +
				"1,3,(T,13)\n" +
				"2,3,(T,23)\n" +
				"3,4,(T,34)\n" +
				"3,5,(T,35)\n" + 
				"4,5,(T,45)\n" + 
				"5,1,(T,51)\n";
			}
			case 5: {
				/*
				 * Test mapEdges() and change the value type to a parameterized custom type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);
				
				DataSet<Edge<Long, DummyCustomParameterizedType<Double>>> mappedEdges = graph.mapEdges(
						new MapFunction<Edge<Long, Long>, DummyCustomParameterizedType<Double>>() {
					public DummyCustomParameterizedType<Double> map(Edge<Long, Long> edge) throws Exception {
						DummyCustomParameterizedType<Double> dummyValue = new DummyCustomParameterizedType<Double>();
						dummyValue.setIntField(edge.getValue().intValue());
						dummyValue.setTField(new Double(edge.getValue()));						
						return dummyValue;
					}
				}).getEdges();
				
				mappedEdges.writeAsCsv(resultPath);
				env.execute();
				return "1,2,(12.0,12)\n" +
				"1,3,(13.0,13)\n" +
				"2,3,(23.0,23)\n" +
				"3,4,(34.0,34)\n" +
				"3,5,(35.0,35)\n" + 
				"4,5,(45.0,45)\n" + 
				"5,1,(51.0,51)\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}

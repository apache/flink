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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomType;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestGraphCreationWithMapper extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 4;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	public TestGraphCreationWithMapper(Configuration config) {
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
				 * Test create() with edge dataset and a mapper that assigns a double constant as value
		         */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					Graph<Long, Double, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongEdgeData(env),
							new MapFunction<Long, Double>() {
								public Double map(Long value) {
									return 0.1d;
								}
							}, env);

					graph.getVertices().writeAsCsv(resultPath);
					env.execute();
					return "1,0.1\n" +
							"2,0.1\n" +
							"3,0.1\n" +
							"4,0.1\n" +
							"5,0.1\n";
				}
				case 2: {
				/*
				 * Test create() with edge dataset and a mapper that assigns a Tuple2 as value
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					Graph<Long, Tuple2<Long, Long>, Long> graph = Graph.fromDataSet(
							TestGraphUtils.getLongLongEdgeData(env), new MapFunction<Long, Tuple2<Long, Long>>() {
								public Tuple2<Long, Long> map(Long vertexId) {
									return new Tuple2<Long, Long>(vertexId*2, 42l);
								}
							}, env);

					graph.getVertices().writeAsCsv(resultPath);
					env.execute();
					return "1,(2,42)\n" +
							"2,(4,42)\n" +
							"3,(6,42)\n" +
							"4,(8,42)\n" +
							"5,(10,42)\n";
				}
				case 3: {
				/*
				 * Test create() with edge dataset with String key type
				 * and a mapper that assigns a double constant as value
				 */
					final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
					Graph<String, Double, Long> graph = Graph.fromDataSet(TestGraphUtils.getStringLongEdgeData(env),
							new MapFunction<String, Double>() {
								public Double map(String value) {
									return 0.1d;
								}
							}, env);

					graph.getVertices().writeAsCsv(resultPath);
					env.execute();
					return "1,0.1\n" +
							"2,0.1\n" +
							"3,0.1\n" +
							"4,0.1\n" +
							"5,0.1\n";
				}
				case 4: {
					/*
					 * Test create() with edge dataset and a mapper that assigns a custom vertex value
					 */
						final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
						Graph<Long, DummyCustomType, Long> graph = Graph.fromDataSet(
								TestGraphUtils.getLongLongEdgeData(env), new MapFunction<Long, DummyCustomType>() {
									public DummyCustomType map(Long vertexId) {
										return new DummyCustomType(vertexId.intValue()-1, false);
									}
								}, env);

						graph.getVertices().writeAsCsv(resultPath);
						env.execute();
						return "1,(F,0)\n" +
								"2,(F,1)\n" +
								"3,(F,2)\n" +
								"4,(F,3)\n" +
								"5,(F,4)\n";
					}
					default:
						throw new IllegalArgumentException("Invalid program id");
				}
			}
		}

	}

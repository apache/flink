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

package org.apache.flink.graph.test.example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.JaccardSimilarityMeasure;
import org.apache.flink.graph.example.NodeSplittingJaccardSimilarityMeasure;
import org.apache.flink.graph.example.utils.JaccardSimilarityMeasureData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.HashSet;
import java.util.TreeSet;

@RunWith(Parameterized.class)
public class JaccardSimilarityMeasureITCase extends MultipleProgramsTestBase {

	private String edgesPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public JaccardSimilarityMeasureITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();

		File edgesFile = tempFolder.newFile();
		Files.write(JaccardSimilarityMeasureData.EDGES, edgesFile, Charsets.UTF_8);

		edgesPath = edgesFile.toURI().toString();
	}

	@Test
	public void testJaccardSimilarityMeasureExample() throws Exception {
		JaccardSimilarityMeasure.main(new String[]{edgesPath, resultPath});
		expected = JaccardSimilarityMeasureData.JACCARD_EDGES;
	}

	@Test
	public void testNodeSplittingJaccardSimilarityMeasureExampleStepOne() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphBeforeFirstStep =
				Graph.fromDataSet(JaccardSimilarityMeasureData.getVerticesBeforeStepOne(env),
						JaccardSimilarityMeasureData.getEdgesBeforeBothSteps(env), env);

		NodeSplittingJaccardSimilarityMeasure.gatherNeighborIds(graphBeforeFirstStep)
				.map(new MapFunction<Vertex<String,Tuple2<String,HashSet<String>>>, Vertex<String,Tuple2<String, TreeSet<String>>>>() {
					@Override
					public Vertex<String, Tuple2<String, TreeSet<String>>> map(Vertex<String, Tuple2<String, HashSet<String>>> value) throws Exception {
						return new Vertex<String, Tuple2<String, TreeSet<String>>>(value.getId(),
								new Tuple2<String, TreeSet<String>>(value.getValue().f0,
										new TreeSet<String>(value.getValue().f1)));
					}
				})
				.writeAsCsv(resultPath, "\n", ",");
		env.execute();

		expected = JaccardSimilarityMeasureData.RESULT_AFTER_STEP_ONE;
	}

	@Test
	public void testNodeSplittingJaccardSimilarityMeasureExampleStepTwo() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<String, Tuple2<String, HashSet<String>>, NullValue> graphBeforeSecondStep =
				Graph.fromDataSet(JaccardSimilarityMeasureData.getVerticesBeforeStepTwo(env),
						JaccardSimilarityMeasureData.getEdgesBeforeBothSteps(env), env);

		NodeSplittingJaccardSimilarityMeasure.compareNeighborSets(graphBeforeSecondStep)
				.writeAsCsv(resultPath, "\n", ",");
		env.execute();

		expected = JaccardSimilarityMeasureData.JACCARD_EDGES;
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}
}

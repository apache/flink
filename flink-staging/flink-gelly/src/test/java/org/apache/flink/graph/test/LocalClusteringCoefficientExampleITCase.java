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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.LocalClusteringCoefficientExample;
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

import java.io.File;

@RunWith(Parameterized.class)
public class LocalClusteringCoefficientExampleITCase extends MultipleProgramsTestBase {

	public LocalClusteringCoefficientExampleITCase(ExecutionMode mode){
		super(mode);
	}

	private String verticesPath;
	private String edgesPath;
    private String resultPath;
    private String expectedResult;

    @Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
		File verticesFile = tempFolder.newFile();
		Files.write(LocalClusteringCoefficientExampleITCase.VERTICES, verticesFile, Charsets.UTF_8);

		File edgesFile = tempFolder.newFile();
		Files.write(LocalClusteringCoefficientExampleITCase.EDGES, edgesFile, Charsets.UTF_8);

		verticesPath = verticesFile.toURI().toString();
		edgesPath = edgesFile.toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testLocalClusteringCoefficientExample() throws Exception {
		LocalClusteringCoefficientExample.main(new String[] {verticesPath, edgesPath, resultPath});

		expectedResult = "1,0.5\n" +
			"2,0.0\n" +
			"3,0.5\n" +
			"4,0.0\n" +
			"5,0.0\n";
	}

	// --------------------------------------------------------------------------------------------
	//  Sample data
	// --------------------------------------------------------------------------------------------

	private static final String VERTICES = "1 1.0\n" +
			"2 2.0\n" +
			"3 3.0\n" +
			"4 4.0\n" +
			"5 5.0\n";

	private static final String EDGES = "1 2 12.0\n" +
			"1 3 13.0\n" +
			"2 3 23.0\n" +
			"3 4 34.0\n" +
			"3 5 35.0\n" +
			"4 5 45.0\n" +
			"5 1 51.0\n";
}

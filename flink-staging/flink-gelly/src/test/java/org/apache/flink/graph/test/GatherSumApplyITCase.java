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
import org.apache.flink.graph.example.GSAPageRank;
import org.apache.flink.graph.example.GSAConnectedComponents;
import org.apache.flink.graph.example.GSASingleSourceShortestPaths;
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
public class GatherSumApplyITCase extends MultipleProgramsTestBase {

	public GatherSumApplyITCase(TestExecutionMode mode){
		super(mode);
	}

	private String edgesPath;
	private String resultPath;
	private String expectedResult;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();

		File edgesFile = tempFolder.newFile();
		Files.write(GatherSumApplyITCase.EDGES, edgesFile, Charsets.UTF_8);

		edgesPath = edgesFile.toURI().toString();

	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	// --------------------------------------------------------------------------------------------
	//  Connected Components Test
	// --------------------------------------------------------------------------------------------

	@Test
	public void testConnectedComponents() throws Exception {
		GSAConnectedComponents.main(new String[]{edgesPath, resultPath, "16"});
		expectedResult = "1 1\n" +
				"2 1\n" +
				"3 1\n" +
				"4 1\n" +
				"5 1\n" +
				"6 6\n" +
				"7 6\n";

	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path Test
	// --------------------------------------------------------------------------------------------

	@Test
	public void testSingleSourceShortestPaths() throws Exception {
		GSASingleSourceShortestPaths.main(new String[]{"1", edgesPath, resultPath, "16"});
		expectedResult = "1 0.0\n" +
				"2 12.0\n" +
				"3 13.0\n" +
				"4 47.0\n" +
				"5 48.0\n" +
				"6 Infinity\n" +
				"7 Infinity\n";
	}

	// --------------------------------------------------------------------------------------------
	//  Page Rank Test
	// --------------------------------------------------------------------------------------------

	@Test
	public void testPageRank() throws Exception {
		GSAPageRank.main(new String[]{edgesPath, resultPath, "16"});
		expectedResult = "1	7.47880014315678E21\n" +
				"2	1.6383884499619055E21\n" +
				"3	3.044048626469292E21\n" +
				"4	1.6896936994425786E21\n" +
				"5	4.214827876275491E21\n" +
				"6	1.0\n" +
				"7	8.157142857142858";
	}

	// --------------------------------------------------------------------------------------------
	//  Sample data
	// --------------------------------------------------------------------------------------------

	private static final String EDGES = "1	2	12.0\n" +
			"1	3	13.0\n" +
			"2	3	23.0\n" +
			"3	4	34.0\n" +
			"3	5	35.0\n" +
			"4	5	45.0\n" +
			"5	1	51.0\n" +
			"6	7	67.0\n";
}

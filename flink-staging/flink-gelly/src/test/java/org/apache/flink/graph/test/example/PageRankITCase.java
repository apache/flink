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

import java.io.File;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import org.apache.flink.graph.example.GSAPageRank;
import org.apache.flink.graph.example.PageRank;
import org.apache.flink.graph.example.utils.PageRankData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PageRankITCase extends MultipleProgramsTestBase {

	public PageRankITCase(TestExecutionMode mode){
		super(mode);
	}

	private String edgesPath;
	private String resultPath;
	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();

		File edgesFile = tempFolder.newFile();
		Files.write(PageRankData.EDGES, edgesFile, Charsets.UTF_8);

		edgesPath = edgesFile.toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareKeyValuePairsWithDelta(expected, resultPath, "\t", 0.01);
	}

	@Test
	public void testPageRankWithThreeIterations() throws Exception {
		PageRank.main(new String[] {edgesPath, resultPath, "3"});
		expected =  PageRankData.RANKS_AFTER_3_ITERATIONS;
	}

	@Test
	public void testGSAPageRankWithThreeIterations() throws Exception {
		GSAPageRank.main(new String[] {edgesPath, resultPath, "3"});
		expected =  PageRankData.RANKS_AFTER_3_ITERATIONS;
	}
}

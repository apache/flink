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
import org.apache.flink.graph.example.CommunityDetection;
import org.apache.flink.graph.example.utils.CommunityDetectionData;
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
public class CommunityDetectionITCase extends MultipleProgramsTestBase {

	private String edgesPath;

	private String resultPath;

	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public CommunityDetectionITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}
	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testSingleIteration() throws Exception {
		/*
		 * Test one iteration of the Simple Community Detection Example
		 */
		final String edges = "1	2	1.0\n" + "1	3	2.0\n" + "1	4	3.0\n" + "1	5	4.0\n" + "2	6	5.0\n" +
				"6	7	6.0\n" + "6	8	7.0\n" + "7	8	8.0";
		edgesPath = createTempFile(edges);

		CommunityDetection.main(new String[]{edgesPath, resultPath, "1",
				CommunityDetectionData.DELTA + ""});

		expected = "1,5\n" + "2,6\n" + "3,1\n" + "4,1\n" + "5,1\n" + "6,8\n" + "7,8\n" + "8,7";
	}

	@Test
	public void testTieBreaker() throws Exception {
		/*
		 * Test one iteration of the Simple Community Detection Example where a tie must be broken
		 */

		final String edges = "1	2	1.0\n" + "1	3	1.0\n" + "1	4	1.0\n" + "1	5	1.0";
		edgesPath = createTempFile(edges);

		CommunityDetection.main(new String[]{edgesPath, resultPath, "1",
				CommunityDetectionData.DELTA + ""});

		expected = "1,2\n" + "2,1\n" + "3,1\n" + "4,1\n" + "5,1";
	}


	// -------------------------------------------------------------------------
	// Util methods
	// -------------------------------------------------------------------------
	private String createTempFile(final String rows) throws Exception {
		File tempFile = tempFolder.newFile();
		Files.write(rows, tempFile, Charsets.UTF_8);
		return tempFile.toURI().toString();
	}
}

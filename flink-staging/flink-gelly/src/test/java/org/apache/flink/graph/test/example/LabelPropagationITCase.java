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
import org.apache.flink.graph.example.LabelPropagation;
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
public class LabelPropagationITCase extends MultipleProgramsTestBase {

	public LabelPropagationITCase(TestExecutionMode mode){
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
	public void testSingleIteration() throws Exception {
		/*
		 * Test one iteration of label propagation example with a simple graph
		 */

		final String vertices = "1	10\n" +
				"2	10\n" +
				"3	30\n" +
				"4	40\n" +
				"5	40\n" +
				"6	40\n" +
				"7	70\n";

		final String edges = "1	3\n" +
				"2	3\n" +
				"4	7\n" +
				"5	7\n" +
				"6	7\n" +
				"7	3\n";

		String verticesPath = createTempFile(vertices);
		String edgesPath = createTempFile(edges);

		LabelPropagation.main(new String[]{verticesPath, edgesPath, resultPath, "1"});

		expectedResult = "1,10\n" +
			"2,10\n" +
			"3,10\n" +
			"4,40\n" +
			"5,40\n" +
			"6,40\n" +
			"7,40\n";
	}

	@Test
	public void testTieBreaker() throws Exception {
		/*
		 * Test the label propagation example where a tie must be broken
		 */

		final String vertices = "1	10\n" +
				"2	10\n" +
				"3	10\n" +
				"4	10\n" +
				"5	0\n" +
				"6	20\n" +
				"7	20\n" +
				"8	20\n" +
				"9	20\n";

		final String edges = "1	5\n" +
				"2	5\n" +
				"3	5\n" +
				"4	5\n" +
				"6	5\n" +
				"7	5\n" +
				"8	5\n" +
				"9	5\n";

		String verticesPath = createTempFile(vertices);
		String edgesPath = createTempFile(edges);

		LabelPropagation.main(new String[]{verticesPath, edgesPath, resultPath, "1"});

		expectedResult = "1,10\n" +
				"2,10\n" +
				"3,10\n" +
				"4,10\n" +
				"5,20\n" +
				"6,20\n" +
				"7,20\n" +
				"8,20\n" +
				"9,20\n";
	}

	// -------------------------------------------------------------------------
	//  Util methods
	// -------------------------------------------------------------------------

	private String createTempFile(final String rows) throws Exception {
		File tempFile = tempFolder.newFile();
		Files.write(rows, tempFile, Charsets.UTF_8);
		return tempFile.toURI().toString();
	}
}

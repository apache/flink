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
import org.apache.flink.graph.example.LabelPropagationExample;
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
public class LabelPropagationExampleITCase extends MultipleProgramsTestBase {

	public LabelPropagationExampleITCase(ExecutionMode mode){
		super(mode);
	}

    private String resultPath;
    private String expectedResult;

	private String verticesPath;
	private String edgesPath;

    @Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();

		final String vertices = "1 1\n" +
				"2 2\n" +
				"3 3\n" +
				"4 4\n" +
				"5 5\n";

		final String edges = "1 2\n" +
				"1 3\n" +
				"2 3\n" +
				"3 4\n" +
				"3 5\n" +
				"4 5\n" +
				"5 1\n";

		File verticesFile = tempFolder.newFile();
		Files.write(vertices, verticesFile, Charsets.UTF_8);

		File edgesFile = tempFolder.newFile();
		Files.write(edges, edgesFile, Charsets.UTF_8);

		verticesPath = verticesFile.toURI().toString();
		edgesPath = edgesFile.toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testLabelPropagation() throws Exception {
		/*
		 * Test the label propagation example
		 */
		LabelPropagationExample.main(new String[] {verticesPath, edgesPath, resultPath, "5", "16"});

		expectedResult = "1,5\n" +
			"2,5\n" +
			"3,5\n" +
			"4,5\n" +
			"5,5\n";
	}
}

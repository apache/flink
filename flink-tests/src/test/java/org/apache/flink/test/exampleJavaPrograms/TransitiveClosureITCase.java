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


package org.apache.flink.test.exampleJavaPrograms;


import java.io.BufferedReader;

import org.apache.flink.examples.java.graph.TransitiveClosureNaive;
import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.test.testdata.TransitiveClosureData;
import org.apache.flink.test.util.JavaProgramTestBase;

public class TransitiveClosureITCase extends JavaProgramTestBase {

	private static final long SEED = 0xBADC0FFEEBEEFL;

	private static final int NUM_VERTICES = 100;

	private static final int NUM_EDGES = 500;

	private String edgesPath;
	private String resultPath;


	@Override
	protected void preSubmit() throws Exception {
		edgesPath = createTempFile("edges.txt", ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}

	@Override
	protected void testProgram() throws Exception {
		TransitiveClosureNaive.main(
				"--edges", edgesPath,
				"--output", resultPath,
				"--iterations", "5");
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			TransitiveClosureData.checkOddEvenResult(reader);
		}
	}
}


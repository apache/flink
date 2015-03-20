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
import org.apache.flink.graph.example.LocalClusteringCoefficientExample;
import org.apache.flink.graph.example.utils.LocalClusteringCoefficientData;
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

	public LocalClusteringCoefficientExampleITCase(TestExecutionMode mode){
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
	public void testSimpleGraph() throws Exception {
		/*
		 * Test Local Clustering Coefficient on the default data
		 */

		String edgesPath = createTempFile(LocalClusteringCoefficientData.EDGES);

		LocalClusteringCoefficientExample.main(new String[] {edgesPath, resultPath});

		expectedResult = "1," + (2/3.0) + "\n" +
				"2,1.0\n" +
				"3,0.5\n" +
				"4,1.0\n" +
				"5," + (2/3.0) + "\n";
	}

	@Test
	public void testNoEdgesBetweenNeighbors() throws Exception {
		/*
		 * Test Local Clustering Coefficient where there are no edges between the neighbors of a node
		 */

		// Generate a 9x9 lattice
		String edges = "";
		expectedResult = "";

		for (int i = 1; i <= 9; ++i) {
			for (int j = 1; j <= 9; ++j) {
				String vertex = Integer.toString(i) + Integer.toString(j);
				expectedResult += vertex + ",0.0\n";

				if (i > 1) {
					String topNeighbor = Integer.toString(i-1) + Integer.toString(j);
					edges += vertex + " " + topNeighbor + "\n";
				}
				if (i < 9) {
					String bottomNeighbor = Integer.toString(i+1) + Integer.toString(j);
					edges += vertex + " " + bottomNeighbor + "\n";
				}
				if (j > 1) {
					String leftNeighbor = Integer.toString(i) + Integer.toString(j-1);
					edges += vertex + " " + leftNeighbor + "\n";
				}
				if (j < 9) {
					String rightNeighbor = Integer.toString(i) + Integer.toString(j+1);
					edges += vertex + " " + rightNeighbor + "\n";
				}
			}
		}

		String edgesPath = createTempFile(edges);

		LocalClusteringCoefficientExample.main(new String[] {edgesPath, resultPath});
	}

	@Test
	public void testAllEdgesBetweenNeighbors() throws Exception {
		/*
		 * Test Local Clustering Coefficient where all edges between the neighbors are present
		 */

		// Generate 3 disjoint K5 graphs
		String edges = "";
		expectedResult = "";

		for (int k = 1; k <= 3; ++k) {
			String component = Integer.toString(k);

			for (int i = 1; i <= 5; ++i) {
				String vertexA = component + Integer.toString(i);
				expectedResult += vertexA + ",1.0\n";

				for (int j = 1; j <= 5; ++j) {
					if (i == j) {
						continue;
					}

					String vertexB = component + Integer.toString(j);

					edges += vertexA + " " + vertexB + "\n";
				}
			}
		}

		String edgesPath = createTempFile(edges);

		LocalClusteringCoefficientExample.main(new String[] {edgesPath, resultPath});
	}

	@Test
	public void testCircleWithCentralVertex() throws Exception {
		/*
		 * Test Local Clustering Coefficient where a vertex in a circle is connected to all vertices of a circle
		 */

		// Generate circle of length 9
		String edges = "";
		expectedResult = "";

		for (int i = 1; i <= 9; ++i) {
			String vertex = Integer.toString(i);
			expectedResult += vertex + "," + Double.toString(2/3.0) + "\n";

			// Connect to neighbors
			if (i > 1) {
				String leftNeighbor = Integer.toString(i - 1);
				edges += vertex + " " + leftNeighbor + "\n";
			} else {
				edges += vertex + " 9\n";
			}

			if (i < 9) {
				String rightNeighbor = Integer.toString(i + 1);
				edges += vertex + " " + rightNeighbor + "\n";
			} else {
				edges += vertex + " 1\n";
			}

			// Connect to the central node
			edges += vertex + " 10\n";
		}

		// Expected result for the central node
		expectedResult += "10,0.25\n";

		String edgesPath = createTempFile(edges);

		LocalClusteringCoefficientExample.main(new String[] {edgesPath, resultPath});
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

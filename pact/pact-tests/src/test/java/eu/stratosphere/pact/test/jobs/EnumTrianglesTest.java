/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.test.jobs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.example.graph.EnumTriangles;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class EnumTrianglesTest extends TestBase {

	String edgesPath = getFilesystemProvider().getTempDirPath() + "/triangleEdges";

	private String edges = "A|B|\n" + "A|C|\n" + "B|C|\n" + "B|D|\n" + "B|E|\n" + "B|F|\n" + "B|I|\n" + "C|D|\n"
		+ "E|F|\n" + "F|G|\n" + "F|I|\n" + "G|H|\n" + "G|J|\n" + "H|I|\n" + "H|J|\n" + "H|K|\n" + "I|K|\n";

	private String expected = "A|B|A|C|B|C|\n" + // A,B,C
		"B|C|B|D|C|D|\n" + // B,C,D
		"B|E|B|F|E|F|\n" + // B,E,F
		"B|F|B|I|F|I|\n" + // B,F,I
		"H|I|H|K|I|K|\n" + // H,I,K
		"G|H|G|J|H|J|\n"; // G,H,J

	public EnumTrianglesTest(Configuration config) {
		super(config);
	}

	@Override
	protected String getJarFilePath() {
		return null;
	}

	@Override
	protected void preSubmit() throws Exception {

		String[] splits = splitInputString(edges, '\n', 4);
		getFilesystemProvider().createDir(edgesPath);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(edgesPath + "/part_" + i + ".txt", splits[i]);
			System.out.println("Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		EnumTriangles enumTriangles = new EnumTriangles();
		Plan plan = enumTriangles.getPlan(edgesPath, getFilesystemProvider().getTempDirPath() + "/triangles.txt", config
			.getString("EnumTrianglesTest#NoSubtasks", "1"));

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		// Test results

		// read result
		InputStream is = getFilesystemProvider().getInputStream(getFilesystemProvider().getTempDirPath() + "/triangles.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String line = reader.readLine();
		Assert.assertNotNull("No output computed", line);

		// collect out lines
		PriorityQueue<String> computedResult = new PriorityQueue<String>();
		while (line != null) {
			computedResult.add(line);
			line = reader.readLine();
		}
		reader.close();

		PriorityQueue<String> expectedResult = new PriorityQueue<String>();
		StringTokenizer st = new StringTokenizer(expected, "\n");
		while (st.hasMoreElements()) {
			expectedResult.add(st.nextToken());
		}

		// print expected and computed results
		System.out.println("Expected: " + expectedResult);
		System.out.println("Computed: " + computedResult);

		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult
			.size());

		while (!expectedResult.isEmpty()) {
			String expectedLine = expectedResult.poll();
			String computedLine = computedResult.poll();
			System.out.println("expLine: <" + expectedLine + ">\t\t: compLine: <" + computedLine + ">");
			Assert.assertEquals("Computed and expected lines differ", expectedLine, computedLine);
		}

		// clean up hdfs
		getFilesystemProvider().delete(edgesPath, true);
		getFilesystemProvider().delete(getFilesystemProvider().getTempDirPath() + "/triangles.txt", false);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("EnumTrianglesTest#NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}

	private String[] splitInputString(String inputString, char splitChar, int noSplits) {

		String splitString = inputString.toString();
		String[] splits = new String[noSplits];
		int partitionSize = (splitString.length() / noSplits) - 2;

		// split data file and copy parts
		for (int i = 0; i < noSplits - 1; i++) {
			int cutPos = splitString.indexOf(splitChar, (partitionSize < splitString.length() ? partitionSize
				: (splitString.length() - 1)));
			splits[i] = splitString.substring(0, cutPos) + "\n";
			splitString = splitString.substring(cutPos + 1);
		}
		splits[noSplits - 1] = splitString;

		return splits;

	}
}

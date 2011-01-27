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

package eu.stratosphere.pact.test.pactPrograms;

import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class EnumTrianglesITCase extends TestBase {

	private static final Log LOG = LogFactory.getLog(EnumTrianglesITCase.class);
	
	String edgesPath = null; 

//	private String edges = "A|B|\n" + "A|C|\n" + "B|C|\n" + "B|D|\n" + "B|E|\n" + "B|F|\n" + "B|I|\n" + "C|D|\n"
//		+ "E|F|\n" + "F|G|\n" + "F|I|\n" + "G|H|\n" + "G|J|\n" + "H|I|\n" + "H|J|\n" + "H|K|\n" + "I|K|\n";
//
//	private String expected = "A|B|A|C|B|C|\n" + // A,B,C
//		"B|C|B|D|C|D|\n" + // B,C,D
//		"B|E|B|F|E|F|\n" + // B,E,F
//		"B|F|B|I|F|I|\n" + // B,F,I
//		"H|I|H|K|I|K|\n" + // H,I,K
//		"G|H|G|J|H|J|\n"; // G,H,J
	
	private String edges = "<a> <foaf:knows> <b>\n" + "<a> <foaf:knows> <c>\n" + "<a> <foaf:knows> <d>\n" + 
						   "<b> <foaf:knows> <c>\n" + "<b> <foaf:knows> <e>\n" + "<b> <foaf:knows> <f>\n" + 
						   "<c> <foaf:knows> <d>\n" + "<d> <foaf:knows> <b>\n" + "<f> <foaf:knows> <g>\n" + 
						   "<f> <foaf:knows> <h>\n" + "<f> <foaf:knows> <i>\n" + "<g> <foaf:knows> <i>\n";

	private String expected = "";
	
	public EnumTrianglesITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {

		edgesPath = getFilesystemProvider().getTempDirPath() + "/triangleEdges";
		
		String[] splits = splitInputString(edges, '\n', 4);
		getFilesystemProvider().createDir(edgesPath);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(edgesPath + "/part_" + i + ".txt", splits[i]);
			LOG.debug("Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		EnumTriangles enumTriangles = new EnumTriangles();
		Plan plan = enumTriangles.getPlan(
				config.getString("EnumTrianglesTest#NoSubtasks", "4"),
				getFilesystemProvider().getURIPrefix() + edgesPath, 
				getFilesystemProvider().getURIPrefix() + getFilesystemProvider().getTempDirPath() + "/triangles.txt");

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		// Test results
		compareResultsByLinesInMemory(expected, getFilesystemProvider().getTempDirPath() + "/triangles.txt");

		// clean up hdfs
		getFilesystemProvider().delete(edgesPath, true);
		getFilesystemProvider().delete(getFilesystemProvider().getTempDirPath() + "/triangles.txt", false);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("EnumTrianglesTest#NoSubtasks", 1);
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

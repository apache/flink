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
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.example.triangles.ComputeEdgeDegrees;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class ComputeEdgeDegreesITCase extends TestBase {

	private static final Log LOG = LogFactory.getLog(ComputeEdgeDegreesITCase.class);
	
	protected String edgesPath = null;
	protected String resultPath = null; 

	private String edges = "1,2\n1,3\n1,4\n1,5\n2,3\n2,5\n3,4\n3,7\n4,3\n6,5\n8,3\n7,8\n5,6\n";
	private String expected = "1,4|2,3\n1,4|3,5\n1,4|4,2\n1,4|5,3\n2,3|3,5\n2,3|5,3\n3,5|4,2\n3,5|7,2\n5,3|6,1\n3,5|8,2\n7,2|8,2\n";
	
	public ComputeEdgeDegreesITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {

		edgesPath = getFilesystemProvider().getTempDirPath() + "/edges";
		resultPath = getFilesystemProvider().getTempDirPath() + "/edgesWithDegrees";
		
		String[] splits = splitInputString(edges, '\n', 4);
		getFilesystemProvider().createDir(edgesPath);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(edgesPath + "/part_" + i + ".txt", splits[i]);
			LOG.debug("Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		ComputeEdgeDegrees computeDegrees = new ComputeEdgeDegrees();
		Plan plan = computeDegrees.getPlan(
				config.getString("EnumTrianglesTest#NoSubtasks", "4"),
				getFilesystemProvider().getURIPrefix() + edgesPath, 
				getFilesystemProvider().getURIPrefix() + resultPath);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		// Test results
		compareResultsByLinesInMemory(expected, resultPath);

		// clean up hdfs
		getFilesystemProvider().delete(edgesPath, true);
		getFilesystemProvider().delete(resultPath, true);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("ComputeEdgeDegreesTest#NoSubtasks", 4);
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

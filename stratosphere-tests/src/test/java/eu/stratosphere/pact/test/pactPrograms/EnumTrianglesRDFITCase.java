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
import eu.stratosphere.pact.example.triangles.EnumTrianglesRdfFoaf;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class EnumTrianglesRDFITCase extends TestBase {

	private static final Log LOG = LogFactory.getLog(EnumTrianglesRDFITCase.class);
	
	String edgesPath = null;
	String resultPath = null; 

	private String edges = "<a> <http://xmlns.com/foaf/0.1/knows> <b>\n" + "<a> <http://xmlns.com/foaf/0.1/knows> <c>\n" + 
						   "<a> <http://xmlns.com/foaf/0.1/knows> <d>\n" + "<b> <http://xmlns.com/foaf/0.1/knows> <c>\n" + 
						   "<b> <http://xmlns.com/foaf/0.1/knows> <e>\n" + "<b> <http://xmlns.com/foaf/0.1/knows> <f>\n" + 
						   "<c> <http://xmlns.com/foaf/0.1/knows> <d>\n" + "<d> <http://xmlns.com/foaf/0.1/knows> <b>\n" + 
						   "<f> <http://xmlns.com/foaf/0.1/knows> <g>\n" + "<f> <http://xmlns.com/foaf/0.1/knows> <h>\n" + 
						   "<f> <http://xmlns.com/foaf/0.1/knows> <i>\n" + "<g> <http://xmlns.com/foaf/0.1/knows> <i>\n" +
						   "<g> <http://willNotWork> <h>\n";

	private String expected = "<a> <b> <c>\n" + "<a> <b> <d>\n" + "<a> <c> <d>\n" + 
	                          "<b> <c> <d>\n" + "<f> <g> <i>\n";
	
	public EnumTrianglesRDFITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {

		edgesPath = getFilesystemProvider().getTempDirPath() + "/triangleEdges";
		resultPath = getFilesystemProvider().getTempDirPath() + "/triangles";
		
		String[] splits = splitInputString(edges, '\n', 4);
		getFilesystemProvider().createDir(edgesPath);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(edgesPath + "/part_" + i + ".txt", splits[i]);
			LOG.debug("Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		EnumTrianglesRdfFoaf enumTriangles = new EnumTrianglesRdfFoaf();
		Plan plan = enumTriangles.getPlan(
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

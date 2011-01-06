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
import eu.stratosphere.pact.example.graph.All2AllSP;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class All2AllSPTest extends TestBase {

	private static final Log LOG = LogFactory.getLog(All2AllSPTest.class);

	String pathsPath = null;

	private String paths = "A|C|7| |\n" + "A|D|6| |\n" + "B|A|1| |\n" + "B|D|2| |\n" + "C|B|3| |\n" + "C|E|10| |\n"
			+ "C|F|12| |\n" + "C|G|9| |\n" + "D|F|5| |\n" + "E|H|2| |\n" + "F|E|3| |\n" + "G|F|1| |\n" + "H|D|2| |\n"
			+ "H|E|4| |\n";

	private String expected = "A|C|7| |\n" + "A|B|10|C|\n" + "A|D|6| |\n" + "A|E|17|C|\n" + "A|F|11|D|\n"
			+ "A|G|16|C|\n" + "B|A|1| |\n" + "B|C|8|A|\n" + "B|D|2| |\n" + "B|F|7|D|\n" + "C|A|4|B|\n" + "C|B|3| |\n"
			+ "C|D|5|B|\n" + "C|E|10| |\n" + "C|F|10|G|\n" + "C|G|9| |\n" + "C|H|12|E|\n" + "D|E|8|F|\n" + "D|F|5| |\n"
			+ "E|D|4|H|\n" + "E|H|2| |\n" + "F|E|3| |\n" + "F|H|5|E|\n" + "G|E|4|F|\n" + "G|F|1| |\n" + "H|D|2| |\n"
			+ "H|E|4| |\n" + "H|F|7|D|\n";

	public All2AllSPTest(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {

		pathsPath = getFilesystemProvider().getTempDirPath() + "/paths";

		getFilesystemProvider().createFile(pathsPath, paths);
		LOG.debug("Paths:\n>" + paths + "<");
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		All2AllSP a2aSP = new All2AllSP();
		Plan plan = a2aSP.getPlan(config.getString("All2AllSPTest#NoSubtasks", "4"), 
				getFilesystemProvider().getURIPrefix() + pathsPath, 
				getFilesystemProvider().getURIPrefix() + getFilesystemProvider().getTempDirPath() + "/iter_1.txt");

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		// Test results
		compareResultsByLinesInMemory(expected, getFilesystemProvider().getTempDirPath() + "/iter_1.txt");

		// clean up hdfs
		getFilesystemProvider().delete(pathsPath, true);
		getFilesystemProvider().delete(getFilesystemProvider().getTempDirPath() + "/iter_1.txt", false);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("All2AllSPTest#NoSubtasks", 4);
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

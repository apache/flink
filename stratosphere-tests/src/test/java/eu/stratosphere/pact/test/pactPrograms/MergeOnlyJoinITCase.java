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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.test.testPrograms.mergeOnlyJoin.MergeOnlyJoin;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class MergeOnlyJoinITCase extends TestBase {

	private static final Log LOG = LogFactory.getLog(MergeOnlyJoinITCase.class);
	
	private String input1Path = null;
	private String input2Path = null;
	private String resultPath = null;

	private final String INPUT1 = "1|9|\n"
		+ "2|8\n"
		+ "3|7\n"
		+ "5|5\n"
		+ "6|4\n"
		+ "7|3\n"
		+ "4|6\n"
		+ "8|2\n"
		+ "2|1\n";
	
	private final String INPUT2 = "2|2|\n"
			+ "2|6|\n"
			+ "2|1|\n"
			+ "4|1|\n"
			+ "5|1|\n"
			+ "2|1|\n";

	
	private final String EXPECTED_RESULT = "2|8|2\n"
			+ "2|8|6\n"
			+ "2|8|1\n"
			+ "2|8|1\n"
			+ "2|1|2\n"
			+ "2|1|6\n"
			+ "2|1|1\n"
			+ "2|1|1\n"
			+ "4|6|1\n"
			+ "5|5|1\n";

	public MergeOnlyJoinITCase(Configuration config) {
		super(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		
		input1Path = getFilesystemProvider().getTempDirPath() + "/input1";
		input2Path = getFilesystemProvider().getTempDirPath() + "/input2";
		resultPath = getFilesystemProvider().getTempDirPath() + "/result";

		String[] splits = splitInputString(INPUT1, '\n', 4);
		getFilesystemProvider().createDir(input1Path);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(input1Path + "/part_" + i + ".txt", splits[i]);
			LOG.debug("Input 1 Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}
		
		splits = splitInputString(INPUT2, '\n', 4);
		getFilesystemProvider().createDir(input2Path);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(input2Path + "/part_" + i + ".txt", splits[i]);
			LOG.debug("Input 2 Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		MergeOnlyJoin mergeOnlyJoin = new MergeOnlyJoin();
		Plan plan = mergeOnlyJoin.getPlan(
				config.getString("MergeOnlyJoinTest#NoSubtasks", "1"), 
				getFilesystemProvider().getURIPrefix()+input1Path,
				getFilesystemProvider().getURIPrefix()+input2Path,
				getFilesystemProvider().getURIPrefix()+resultPath,
				config.getString("MergeOnlyJoinTest#NoSubtasksInput2", "1"));

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
	
	@Override
	public void stopCluster() throws Exception {
		getFilesystemProvider().delete(input1Path, true);
		getFilesystemProvider().delete(input2Path, true);
		getFilesystemProvider().delete(resultPath, true);
		super.stopCluster();
	}
	

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		ArrayList<Configuration> tConfigs = new ArrayList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("MergeOnlyJoinTest#NoSubtasks", 3);
		config.setInteger("MergeOnlyJoinTest#NoSubtasksInput2", 3);
		tConfigs.add(config);

		config = new Configuration();
		config.setInteger("MergeOnlyJoinTest#NoSubtasks", 3);
		config.setInteger("MergeOnlyJoinTest#NoSubtasksInput2", 4);
		tConfigs.add(config);

		config = new Configuration();
		config.setInteger("MergeOnlyJoinTest#NoSubtasks", 3);
		config.setInteger("MergeOnlyJoinTest#NoSubtasksInput2", 2);
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
			if (cutPos != -1) {
				splits[i] = splitString.substring(0, cutPos) + "\n";
				splitString = splitString.substring(cutPos + 1);	
			}
			else {
				splits[i] = "";
			}
		}
		splits[noSplits - 1] = splitString;
		return splits;
	}
}

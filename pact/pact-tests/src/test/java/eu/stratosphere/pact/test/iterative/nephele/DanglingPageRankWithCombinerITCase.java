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

package eu.stratosphere.pact.test.iterative.nephele;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.custom.CustomCompensatableDanglingPageRankWithCombiner;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class DanglingPageRankWithCombinerITCase extends TestBase {
	
	protected String pagesWithRankPath;
	protected String edgesPath;
	protected String resultPath;

	
	public DanglingPageRankWithCombinerITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {

		this.pagesWithRankPath = getFilesystemProvider().getTempDirPath() + "/pagesWithRank";
		this.edgesPath = getFilesystemProvider().getTempDirPath() + "/edges";
		this.resultPath = getFilesystemProvider().getTempDirPath() + "/result";
		
		final int numPartitions = 2;

		// create data path
		getFilesystemProvider().createDir(this.pagesWithRankPath);
		getFilesystemProvider().createDir(this.edgesPath);
		getFilesystemProvider().createDir(this.resultPath);
		
		String[] vertexSplits = splitInputString(DanglingPageRankITCase.TEST_VERTICES, '\n', numPartitions);
		String[] edgesSplits = splitInputString(DanglingPageRankITCase.TEST_EDGES, '\n', numPartitions);

		for (int i = 0; i < numPartitions; i++) {
			getFilesystemProvider().createFile(pagesWithRankPath + "/part_" + i + ".txt", vertexSplits[i]);
			getFilesystemProvider().createFile(edgesPath + "/part_" + i + ".txt", edgesSplits[i]);
		}
	}
	
	@Override
	protected void postSubmit() throws Exception {
		// clean up file
		getFilesystemProvider().delete(this.pagesWithRankPath, true);
		getFilesystemProvider().delete(this.edgesPath, true);
		getFilesystemProvider().delete(this.resultPath, true);

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		
		String[] parameters = new String[] {
			"4",
			"4",
			getFilesystemProvider().getURIPrefix() + pagesWithRankPath,
			getFilesystemProvider().getURIPrefix() + edgesPath,
			getFilesystemProvider().getURIPrefix() + resultPath,
			"<none>",
			"5",
			"20",
			"15",
			"30",
			"5",
			"1",
			"0",
			"100",
			"0"
		};
		
		return CustomCompensatableDanglingPageRankWithCombiner.getJobGraph(parameters);
	}


	@Parameters
	public static Collection<Object[]> getConfigurations() {
		ArrayList<Configuration> tConfigs = new ArrayList<Configuration>();
		tConfigs.add(new Configuration());
		return toParameterList(tConfigs);
	}
	

	private String[] splitInputString(String splitString, char splitChar, int noSplits) {
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

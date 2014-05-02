/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.exampleRecordPrograms;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.testPrograms.mergeOnlyJoin.MergeOnlyJoin;
import eu.stratosphere.test.util.RecordAPITestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class MergeOnlyJoinITCase extends RecordAPITestBase {

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
		input1Path = createTempFile("input1.txt", INPUT1);
		input2Path = createTempFile("input2.txt", INPUT2);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		MergeOnlyJoin mergeOnlyJoin = new MergeOnlyJoin();
		return mergeOnlyJoin.getPlan(
				config.getString("MergeOnlyJoinTest#NoSubtasks", "1"), 
				input1Path,
				input2Path,
				resultPath,
				config.getString("MergeOnlyJoinTest#NoSubtasksInput2", "1"));
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
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
}

/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.test.hadoopcompatibility;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.hadoopcompatibility.example.WordCountWithHadoopOutputFormat;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.TestBase2;

/**
 * test the hadoop inputformat and outputformat for stratosphere
 */
public class HadoopInputOutputTest extends TestBase2 {
	protected String textPath;
	protected String resultPath;
	protected String counts;

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
		counts = WordCountData.COUNTS.replaceAll(" ", "\t");
	}

	@Override
	protected Plan getTestJob() {
		//WordCountWithHadoopOutputFormat takes hadoop TextInputFormat as input and output file in hadoop TextOutputFormat
		WordCountWithHadoopOutputFormat wc = new WordCountWithHadoopOutputFormat();
		return wc.getPlan("1", textPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results, append /1 to resultPath due to the generated _temproray file.
		compareResultsByLinesInMemory(counts, resultPath + "/1");
	}
}

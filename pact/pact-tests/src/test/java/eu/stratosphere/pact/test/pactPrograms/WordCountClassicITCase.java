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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.example.wordcount.WordCountClassic;
import eu.stratosphere.pact.test.util.TestBase2;

@RunWith(Parameterized.class)
public class WordCountClassicITCase extends TestBase2 {

	private static final String TEXT = WordCountITCase.TEXT;

	private static final String COUNTS = WordCountITCase.COUNTS;

	protected String textPath;
	protected String resultPath;

	
	public WordCountClassicITCase(Configuration config) {
		super(config);
	}

	
	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getPactPlan() {
		WordCountClassic wc = new WordCountClassic();
		return wc.getPlan(config.getString("WordCountTest#NumSubtasks", "1"),
				textPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		// Test results
		compareResultsByLinesInMemory(COUNTS, resultPath);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("WordCountTest#NumSubtasks", 4);
		return toParameterList(config);
	}
}

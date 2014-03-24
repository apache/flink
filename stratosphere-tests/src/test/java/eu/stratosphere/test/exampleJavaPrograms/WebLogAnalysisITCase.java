/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.test.exampleJavaPrograms;


import eu.stratosphere.example.java.relational.WebLogAnalysis;
import eu.stratosphere.test.testdata.WebLogAnalysisData;
import eu.stratosphere.test.util.JavaProgramTestBase;

public class WebLogAnalysisITCase extends JavaProgramTestBase {

	private String docsPath;
	private String ranksPath;
	private String visitsPath;
	private String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		docsPath = createTempFile("docs", WebLogAnalysisData.DOCS);
		ranksPath = createTempFile("ranks", WebLogAnalysisData.RANKS);
		visitsPath = createTempFile("visits", WebLogAnalysisData.VISITS);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WebLogAnalysisData.EXCEPTED_RESULT, resultPath);
	}
	@Override
	protected void testProgram() throws Exception {
		WebLogAnalysis.main(new String[] {docsPath, ranksPath, visitsPath, resultPath});
	}
}

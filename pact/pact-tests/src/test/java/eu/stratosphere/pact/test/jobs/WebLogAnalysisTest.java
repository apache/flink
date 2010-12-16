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

package eu.stratosphere.pact.test.jobs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.example.relational.WebLogAnalysis;
import eu.stratosphere.pact.test.util.TestBase;

@RunWith(Parameterized.class)
public class WebLogAnalysisTest extends TestBase {

	String docsPath = getHDFSProvider().getHdfsHome() + "/docs";

	String ranksPath = getHDFSProvider().getHdfsHome() + "/ranks";

	String visitsPath = getHDFSProvider().getHdfsHome() + "/visits";

	// TODO: copy test data from generator
	String docs = "aaaaa|Bombs Allah Dschihad Blubb Bla asdfas a dsfas asdfasdf asdfas|\n"
		+ "aaaab|ASd adsf asdf asdfasg a asdfgasf adfasdf asdfasd asdf adfasda|\n"
		+ "aaaac|adsfasd Bombs asdtwafs Allah asdf asdfa asdfas Dschihad asdfa|\n"
		+ "aaaad|adfas asdasd a das asdfasd asdfasd Bombs dghwasgasdha gaefeea|\n"
		+ "aaaae|adfas asdasd a das asdfasd asdfasd aatafa ghwasgasdha gaefeea|\n"
		+ "aaaaf|adsfasd Bombs asdtwafs Allah asdf asdfa asdfas Dschihad asdfa|\n"
		+ "aaaag|adsfasd Bombs asdtwafs Allah asdf asdfa asdfas Dschihad asdfa|\n"
		+ "aaaah|adfas asdasd a das asdfasd asdfasd aatafa ghwasgasdha gaefeea|\n"
		+ "aaaai|adsfasd Bombs asdtwafs Allah asdf asdfa asdfas Dschihad asdfa|\n"
		+ "aaaaj|adfas asdasd a das asdfasd Allah Dschihad ghwasgasdha gaefeea|\n"
		+ "aaaak|adsfasd Bombs asdtwafs Allah asdf asdfa asdfas Dschihad asdfa|\n"
		+ "aaaal|adsfasd Bombs asdtwafs Allah asdf asdfa asdfas Dschihad asdfa|\n";

	String ranks = "10|aaaaa|626|\n" + "80|aaaab|132|\n" + "51|aaaac|63|\n" + "25|aaaad|114|\n" + "12|aaaae|634|\n"
		+ "68|aaaaf|832|\n" + "74|aaaag|42|\n" + "22|aaaah|62|\n" + "43|aaaai|82|\n" + "28|aaaaj|62|\n"
		+ "99|aaaak|783|\n" + "76|aaaal|275|\n";

	String visits = "12.12.12.124|aaaaa|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaab|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaaf|2010-02-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaah|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaae|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaak|2010-05-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaal|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaab|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaaa|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaac|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaal|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaao|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaas|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaab|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaac|2010-02-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaaf|2010-03-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaah|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaag|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaai|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaad|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaaf|2010-04-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaaf|2010-05-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaaf|2010-06-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaag|2010-04-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaai|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaak|2010-06-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaaa|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaal|2010-02-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n"
		+ "12.12.12.124|aaaab|2010-01-01|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n";

	String expectedResult = "aaaaf|68|832|\n" + "aaaak|99|783|\n";

	public WebLogAnalysisTest(Configuration config) {
		super(config);
	}

	@Override
	protected String getJarFilePath() {
		return null;
	}

	@Override
	protected void preSubmit() throws Exception {

		String[] splits = splitInputString(docs, '\n', 4);
		getHDFSProvider().createDir(docsPath);
		for (int i = 0; i < splits.length; i++) {
			getHDFSProvider().writeFileToHDFS(docsPath + "/part_" + i + ".txt", splits[i]);
			System.out.println("Docs Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

		splits = splitInputString(ranks, '\n', 4);
		getHDFSProvider().createDir(ranksPath);
		for (int i = 0; i < splits.length; i++) {
			getHDFSProvider().writeFileToHDFS(ranksPath + "/part_" + i + ".txt", splits[i]);
			System.out.println("Ranks Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

		splits = splitInputString(visits, '\n', 4);
		getHDFSProvider().createDir(visitsPath);
		for (int i = 0; i < splits.length; i++) {
			getHDFSProvider().writeFileToHDFS(visitsPath + "/part_" + i + ".txt", splits[i]);
			System.out.println("Visits Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		WebLogAnalysis relOLAP = new WebLogAnalysis();
		Plan plan = relOLAP.getPlan(config.getString("WebLogAnalysisTest#NoSubtasks", "1"),
			docsPath, ranksPath, visitsPath, getHDFSProvider().getHdfsHome() + "/result.txt");

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		// Test results

		// read result
		InputStream is = getHDFSProvider().getHdfsInputStream(getHDFSProvider().getHdfsHome() + "/result.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String line = reader.readLine();
		Assert.assertNotNull("No output computed", line);

		// collect out lines
		PriorityQueue<String> computedResult = new PriorityQueue<String>();
		while (line != null) {
			computedResult.add(line);
			line = reader.readLine();
		}
		reader.close();

		PriorityQueue<String> expectedResult = new PriorityQueue<String>();
		StringTokenizer st = new StringTokenizer(this.expectedResult, "\n");
		while (st.hasMoreElements()) {
			expectedResult.add(st.nextToken());
		}

		// print expected and computed results
		System.out.println("Expected: " + expectedResult);
		System.out.println("Computed: " + computedResult);

		Assert.assertEquals("Computed and expected results have different size", expectedResult.size(), computedResult
			.size());

		while (!expectedResult.isEmpty()) {
			String expectedLine = expectedResult.poll();
			String computedLine = computedResult.poll();
			System.out.println("expLine: <" + expectedLine + ">\t\t: compLine: <" + computedLine + ">");
			Assert.assertEquals("Computed and expected lines differ", expectedLine, computedLine);
		}

		// clean up hdfs
		getHDFSProvider().delete(docsPath, true);
		getHDFSProvider().delete(ranksPath, true);
		getHDFSProvider().delete(visitsPath, true);
		getHDFSProvider().delete(getHDFSProvider().getHdfsHome() + "/result.txt", true);

	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("WebLogAnalysisTest#NoSubtasks", 4);
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

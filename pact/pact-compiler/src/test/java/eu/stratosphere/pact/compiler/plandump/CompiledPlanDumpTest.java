/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.compiler.plandump;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.CompilerTestBase;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.example.datamining.KMeansIteration;
import eu.stratosphere.pact.example.relational.TPCHQuery3;
import eu.stratosphere.pact.example.relational.WebLogAnalysis;
import eu.stratosphere.pact.example.wordcount.WordCount;


public class CompiledPlanDumpTest extends CompilerTestBase {
	
	@Test
	public void dumpWordCount() {
		dump(new WordCount().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE_1, OUT_FILE_1));
	}
	
	@Test
	public void dumpTPCH3() {
		dump(new TPCHQuery3().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE_1, IN_FILE_1, OUT_FILE_1));
	}
	
	@Test
	public void dumpKMeans() {
		dump(new KMeansIteration().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE_1, IN_FILE_1, OUT_FILE_1));
	}
	
	@Test
	public void dumpWebLogAnalysis() {
		dump(new WebLogAnalysis().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE_1, IN_FILE_1, IN_FILE_1, OUT_FILE_1));
	}
	
	private void dump(Plan p) {
		try {
			OptimizedPlan op = compile(p);
			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			PrintWriter writer = new PrintWriter(getNullOrTempFile());
			
//			StringWriter sw = new StringWriter(512);
//			PrintWriter writer = new PrintWriter(sw, true);
			dumper.dumpOptimizerPlanAsJSON(op, writer);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An error occurred in the test: " + e.getMessage());
		}
	}
	
	private File getNullOrTempFile() throws IOException {
		File f = new File("/dev/null");
		if (f.exists() && f.canWrite()) {
			return f;
		} else {
			File tmp = File.createTempFile("dump", ".json");
			tmp.deleteOnExit();
			return tmp;
		}
	}
}

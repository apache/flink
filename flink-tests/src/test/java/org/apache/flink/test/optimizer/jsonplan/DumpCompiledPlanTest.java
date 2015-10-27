/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.optimizer.jsonplan;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PreviewPlanEnvironment;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.examples.java.clustering.KMeans;
import org.apache.flink.examples.java.graph.ConnectedComponents;
import org.apache.flink.examples.java.graph.PageRankBasic;
import org.apache.flink.examples.java.relational.TPCHQuery3;
import org.apache.flink.examples.java.relational.WebLogAnalysis;
import org.apache.flink.examples.java.wordcount.WordCount;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

/*
 * The tests in this class simply invokes the JSON dump code for the optimized plan.
 */
public class DumpCompiledPlanTest extends CompilerTestBase {
	
	@Test
	public void dumpWordCount() {
		// prepare the test environment
		PreviewPlanEnvironment env = new PreviewPlanEnvironment();
		env.setAsContext();
		try {
			WordCount.main(new String[] {IN_FILE, OUT_FILE});
		} catch(OptimizerPlanEnvironment.ProgramAbortException pae) {
			// all good.
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("WordCount failed with an exception");
		}
		dump(env.getPlan());
	}
	
	@Test
	public void dumpTPCH3() {
		// prepare the test environment
		PreviewPlanEnvironment env = new PreviewPlanEnvironment();
		env.setAsContext();
		try {
			TPCHQuery3.main(new String[] {IN_FILE, IN_FILE, OUT_FILE, "123"});
		} catch(OptimizerPlanEnvironment.ProgramAbortException pae) {
			// all good.
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("TPCH3 failed with an exception");
		}
		dump(env.getPlan());
	}
	
	@Test
	public void dumpIterativeKMeans() {
		// prepare the test environment
		PreviewPlanEnvironment env = new PreviewPlanEnvironment();
		env.setAsContext();
		try {
			KMeans.main(new String[] {IN_FILE, IN_FILE, OUT_FILE, "123"});
		} catch(OptimizerPlanEnvironment.ProgramAbortException pae) {
			// all good.
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("KMeans failed with an exception");
		}
		dump(env.getPlan());
	}
	
	@Test
	public void dumpWebLogAnalysis() {
		// prepare the test environment
		PreviewPlanEnvironment env = new PreviewPlanEnvironment();
		env.setAsContext();
		try {
			WebLogAnalysis.main(new String[] {IN_FILE, IN_FILE, OUT_FILE, "123"});
		} catch(OptimizerPlanEnvironment.ProgramAbortException pae) {
			// all good.
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("WebLogAnalysis failed with an exception");
		}
		dump(env.getPlan());
	}

	@Test
	public void dumpBulkIterationKMeans() {
		// prepare the test environment
		PreviewPlanEnvironment env = new PreviewPlanEnvironment();
		env.setAsContext();
		try {
			ConnectedComponents.main(new String[] {IN_FILE, IN_FILE, OUT_FILE, "123"});
		} catch(OptimizerPlanEnvironment.ProgramAbortException pae) {
			// all good.
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("ConnectedComponents failed with an exception");
		}
		dump(env.getPlan());
	}
	
	@Test
	public void dumpPageRank() {
		// prepare the test environment
		PreviewPlanEnvironment env = new PreviewPlanEnvironment();
		env.setAsContext();
		try {
			PageRankBasic.main(new String[] {IN_FILE, IN_FILE, OUT_FILE, "10", "123"});
		} catch(OptimizerPlanEnvironment.ProgramAbortException pae) {
			// all good.
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("PagaRank failed with an exception");
		}
		dump(env.getPlan());
	}
	
	private void dump(Plan p) {
		p.setExecutionConfig(new ExecutionConfig());
		try {
			OptimizedPlan op = compileNoStats(p);
			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			String json = dumper.getOptimizerPlanAsJSON(op);
			JsonParser parser = new JsonFactory().createJsonParser(json);
			while (parser.nextToken() != null);
		} catch (JsonParseException e) {
			e.printStackTrace();
			Assert.fail("JSON Generator produced malformatted output: " + e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An error occurred in the test: " + e.getMessage());
		}
	}
}

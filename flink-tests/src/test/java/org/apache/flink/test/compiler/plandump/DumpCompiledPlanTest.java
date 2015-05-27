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

package org.apache.flink.test.compiler.plandump;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.program.Client.ProgramAbortException;
import org.apache.flink.client.program.PackagedProgram.PreviewPlanEnvironment;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.examples.java.clustering.KMeans;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.test.recordJobs.graph.DeltaPageRankWithInitialDeltas;
import org.apache.flink.test.recordJobs.kmeans.KMeansBroadcast;
import org.apache.flink.test.recordJobs.kmeans.KMeansSingleStep;
import org.apache.flink.test.recordJobs.relational.TPCHQuery3;
import org.apache.flink.test.recordJobs.relational.WebLogAnalysis;
import org.apache.flink.test.recordJobs.wordcount.WordCount;
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
		dump(new WordCount().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, OUT_FILE));
	}
	
	@Test
	public void dumpTPCH3() {
		dump(new TPCHQuery3().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE));
	}
	
	@Test
	public void dumpKMeans() {
		dump(new KMeansSingleStep().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE));
	}
	
	@Test
	public void dumpIterativeKMeans() {
		// prepare the test environment
		PreviewPlanEnvironment env = new PreviewPlanEnvironment();
		env.setAsContext();
		try {
			// <points path> <centers path> <result path> <num iterations
			KMeans.main(new String[] {IN_FILE, IN_FILE, OUT_FILE, "123"});
		} catch(ProgramAbortException pae) {
			// all good.
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("KMeans failed with an exception");
		}
		dump(env.getPlan());
	}
	
	@Test
	public void dumpWebLogAnalysis() {
		dump(new WebLogAnalysis().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, IN_FILE, OUT_FILE));
	}

	@Test
	public void dumpBulkIterationKMeans() {
		dump(new KMeansBroadcast().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, OUT_FILE));
	}
	
	@Test
	public void dumpDeltaPageRank() {
		dump(new DeltaPageRankWithInitialDeltas().getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, IN_FILE, OUT_FILE, "10"));
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

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

import org.apache.flink.api.common.Plan;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PreviewPlanEnvironment;
import org.apache.flink.examples.java.clustering.KMeans;
import org.apache.flink.examples.java.graph.ConnectedComponents;
import org.apache.flink.examples.java.graph.PageRank;
import org.apache.flink.examples.java.relational.TPCHQuery3;
import org.apache.flink.examples.java.wordcount.WordCount;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * The tests in this class simply invokes the JSON dump code for the original plan.
 */
public class PreviewPlanDumpTest extends CompilerTestBase {

	@Test
	public void dumpWordCount() {
		// prepare the test environment
		PreviewPlanEnvironment env = new PreviewPlanEnvironment();
		env.setAsContext();
		try {
			WordCount.main(new String[] {
					"--input", IN_FILE,
					"--output", OUT_FILE});
		} catch (OptimizerPlanEnvironment.ProgramAbortException pae) {
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
			TPCHQuery3.main(new String[] {
					"--lineitem", IN_FILE,
					"--customer", IN_FILE,
					"--orders", OUT_FILE,
					"--output", "123"});
		} catch (OptimizerPlanEnvironment.ProgramAbortException pae) {
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
			KMeans.main(new String[] {
				"--points ", IN_FILE,
				"--centroids ", IN_FILE,
				"--output ", OUT_FILE,
				"--iterations", "123"});
		} catch (OptimizerPlanEnvironment.ProgramAbortException pae) {
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
			org.apache.flink.examples.java.relational.WebLogAnalysis.main(new String[] {
					"--documents", IN_FILE,
					"--ranks", IN_FILE,
					"--visits", OUT_FILE,
					"--output", "123"});
		} catch (OptimizerPlanEnvironment.ProgramAbortException pae) {
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
			ConnectedComponents.main(new String[] {
					"--vertices", IN_FILE,
					"--edges", IN_FILE,
					"--output", OUT_FILE,
					"--iterations", "123"});
		} catch (OptimizerPlanEnvironment.ProgramAbortException pae) {
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
			// --pages <path> --links <path> --output <path> --numPages <n> --iterations <n>
			PageRank.main(new String[]{
					"--pages", IN_FILE,
					"--links", IN_FILE,
					"--output", OUT_FILE,
					"--numPages", "10",
					"--iterations", "123"});
		} catch (OptimizerPlanEnvironment.ProgramAbortException pae) {
			// all good.
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("PageRank failed with an exception");
		}
		dump(env.getPlan());
	}

	private void dump(Plan p) {
		try {
			List<DataSinkNode> sinks = Optimizer.createPreOptimizedPlan(p);
			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			String json = dumper.getPactPlanAsJSON(sinks);
			try (JsonParser parser = new JsonFactory().createParser(json)) {
				while (parser.nextToken() != null) {}
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
			Assert.fail("JSON Generator produced malformatted output: " + e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An error occurred in the test: " + e.getMessage());
		}
	}
}

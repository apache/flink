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
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.clustering.KMeans;
import org.apache.flink.examples.java.graph.ConnectedComponents;
import org.apache.flink.examples.java.graph.PageRank;
import org.apache.flink.examples.java.relational.TPCHQuery3;
import org.apache.flink.examples.java.relational.WebLogAnalysis;
import org.apache.flink.examples.java.wordcount.WordCount;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * The tests in this class simply invokes the JSON dump code for the optimized plan.
 */
public class DumpCompiledPlanTest extends CompilerTestBase {

	@Test
	public void dumpWordCount() throws Exception {
		verifyOptimizedPlan(WordCount.class,
			"--input", IN_FILE,
			"--output", OUT_FILE);
	}

	@Test
	public void dumpTPCH3() throws Exception {
		verifyOptimizedPlan(TPCHQuery3.class,
			"--lineitem", IN_FILE,
			"--customer", IN_FILE,
			"--orders", OUT_FILE,
			"--output", "123");
	}

	@Test
	public void dumpIterativeKMeans() throws Exception {
		verifyOptimizedPlan(KMeans.class,
			"--points ", IN_FILE,
			"--centroids ", IN_FILE,
			"--output ", OUT_FILE,
			"--iterations", "123");
	}

	@Test
	public void dumpWebLogAnalysis() throws Exception {
		verifyOptimizedPlan(WebLogAnalysis.class,
			"--documents", IN_FILE,
			"--ranks", IN_FILE,
			"--visits", OUT_FILE,
			"--output", "123");
	}

	@Test
	public void dumpBulkIterationKMeans() throws Exception {
		verifyOptimizedPlan(ConnectedComponents.class,
			"--vertices", IN_FILE,
			"--edges", IN_FILE,
			"--output", OUT_FILE,
			"--iterations", "123");
	}

	@Test
	public void dumpPageRank() throws Exception {
		verifyOptimizedPlan(PageRank.class,
			"--pages", IN_FILE,
			"--links", IN_FILE,
			"--output", OUT_FILE,
			"--numPages", "10",
			"--iterations", "123");
	}

	private void verifyOptimizedPlan(Class<?> entrypoint, String... args) throws Exception {
		final PackagedProgram program = PackagedProgram
			.newBuilder()
			.setEntryPointClassName(entrypoint.getName())
			.setArguments(args)
			.build();

		final Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(program, new Configuration(), 1, true);

		assertTrue(pipeline instanceof Plan);

		final Plan plan = (Plan) pipeline;

		final OptimizedPlan op = compileNoStats(plan);
		final PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
		final String json = dumper.getOptimizerPlanAsJSON(op);
		try (JsonParser parser = new JsonFactory().createParser(json)) {
			while (parser.nextToken() != null) {
			}
		}
	}
}

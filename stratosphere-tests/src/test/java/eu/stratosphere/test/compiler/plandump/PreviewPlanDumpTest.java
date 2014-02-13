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
package eu.stratosphere.test.compiler.plandump;

import java.util.List;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.dag.DataSinkNode;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.example.java.record.connectedcomponents.WorksetConnectedComponents;
import eu.stratosphere.example.java.record.kmeans.KMeans;
import eu.stratosphere.example.java.record.kmeans.KMeansSingleStep;
import eu.stratosphere.example.java.record.relational.TPCHQuery3;
import eu.stratosphere.example.java.record.relational.WebLogAnalysis;
import eu.stratosphere.example.java.record.wordcount.WordCount;
import eu.stratosphere.util.OperatingSystem;

/*
 * The tests in this class simply invokes the JSON dump code for the original plan.
 */
public class PreviewPlanDumpTest {
	
	protected static final String IN_FILE = OperatingSystem.isWindows() ?  "file:/c:/test/file" : "file:///test/file";
	
	protected static final String OUT_FILE = OperatingSystem.isWindows() ?  "file:/c:/test/output" : "file:///test/output";
	
	protected static final String[] NO_ARGS = new String[0];
	
	@Test
	public void dumpWordCount() {
		dump(new WordCount().getPlan("4", IN_FILE, OUT_FILE));
		
		// The web interface passes empty string-args to compute the preview of the
		// job, so we should test this situation too
		dump(new WordCount().getPlan(NO_ARGS));
	}
	
	@Test
	public void dumpTPCH3() {
		dump(new TPCHQuery3().getPlan("4", IN_FILE, IN_FILE, OUT_FILE));
		dump(new TPCHQuery3().getPlan(NO_ARGS));
	}
	
	@Test
	public void dumpKMeans() {
		dump(new KMeansSingleStep().getPlan("4", IN_FILE, IN_FILE, OUT_FILE));
		dump(new KMeansSingleStep().getPlan(NO_ARGS));
	}
	
	@Test
	public void dumpWebLogAnalysis() {
		dump(new WebLogAnalysis().getPlan("4", IN_FILE, IN_FILE, IN_FILE, OUT_FILE));
		dump(new WebLogAnalysis().getPlan(NO_ARGS));
	}
	
	@Test
	public void dumpBulkIterationKMeans() {
		dump(new KMeans().getPlan("4", IN_FILE, OUT_FILE));
		dump(new KMeans().getPlan(NO_ARGS));
	}
	
	@Test
	public void dumpWorksetConnectedComponents() {
		dump(new WorksetConnectedComponents().getPlan("4", IN_FILE, IN_FILE, OUT_FILE));
		dump(new WorksetConnectedComponents().getPlan(NO_ARGS));
	}
	
	private void dump(Plan p) {
		try {
			List<DataSinkNode> sinks = PactCompiler.createPreOptimizedPlan(p);
			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			String json = dumper.getPactPlanAsJSON(sinks);
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

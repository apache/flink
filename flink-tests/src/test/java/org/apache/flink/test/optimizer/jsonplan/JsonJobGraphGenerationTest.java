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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.clustering.KMeans;
import org.apache.flink.examples.java.graph.ConnectedComponents;
import org.apache.flink.examples.java.relational.WebLogAnalysis;
import org.apache.flink.examples.java.wordcount.WordCount;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test job graph generation in JSON format.
 */
public class JsonJobGraphGenerationTest {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private PrintStream out;
	private PrintStream err;

	@Before
	public void redirectStreams() {
		this.out = System.out;
		this.err = System.err;

		OutputStream discards = new OutputStream() {
			@Override
			public void write(int b) {}
		};

		System.setOut(new PrintStream(discards));
		System.setErr(new PrintStream(discards));
	}

	@After
	public void restoreStreams() {
		if (out != null) {
			System.setOut(out);
		}
		if (err != null) {
			System.setOut(err);
		}
	}

	@Test
	public void testWordCountPlan() {
		try {
			// without arguments
			try {
				final int parallelism = 1; // some ops have DOP 1 forced
				JsonValidator validator = new GenericValidator(parallelism, 3);
				TestingExecutionEnvironment.setAsNext(validator, parallelism);

				WordCount.main(new String[0]);
			}
			catch (AbortError ignored) {}

			// with arguments
			try {
				final int parallelism = 17;
				JsonValidator validator = new GenericValidator(parallelism, 3);
				TestingExecutionEnvironment.setAsNext(validator, parallelism);

				String tmpDir = tempFolder.newFolder().getAbsolutePath();
				WordCount.main(new String[] {
						"--input", tmpDir,
						"--output", tmpDir});
			}
			catch (AbortError ignored) {}
		}
		catch (Exception e) {
			restoreStreams();
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testWebLogAnalysis() {
		try {
			// without arguments
			try {
				final int parallelism = 1; // some ops have DOP 1 forced
				JsonValidator validator = new GenericValidator(parallelism, 6);
				TestingExecutionEnvironment.setAsNext(validator, parallelism);

				WebLogAnalysis.main(new String[0]);
			}
			catch (AbortError ignored) {}

			// with arguments
			try {
				final int parallelism = 17;
				JsonValidator validator = new GenericValidator(parallelism, 6);
				TestingExecutionEnvironment.setAsNext(validator, parallelism);

				String tmpDir = tempFolder.newFolder().getAbsolutePath();
				WebLogAnalysis.main(new String[] {
						"--documents", tmpDir,
						"--ranks", tmpDir,
						"--visits", tmpDir,
						"--output", tmpDir});
			}
			catch (AbortError ignored) {}
		}
		catch (Exception e) {
			restoreStreams();
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testKMeans() {
		try {
			// without arguments
			try {
				final int parallelism = 1; // some ops have DOP 1 forced
				JsonValidator validator = new GenericValidator(parallelism, 9);
				TestingExecutionEnvironment.setAsNext(validator, parallelism);

				KMeans.main(new String[0]);
			}
			catch (AbortError ignored) {}

			// with arguments
			try {
				final int parallelism = 42;
				JsonValidator validator = new GenericValidator(parallelism, 9);
				TestingExecutionEnvironment.setAsNext(validator, parallelism);

				String tmpDir = tempFolder.newFolder().getAbsolutePath();
				KMeans.main(new String[] {
					"--points", tmpDir,
					"--centroids", tmpDir,
					"--output", tmpDir,
					"--iterations", "100"});
			}
			catch (AbortError ignored) {}

		}
		catch (Exception e) {
			restoreStreams();
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testConnectedComponents() {
		try {
			// without arguments
			try {
				final int parallelism = 1; // some ops have DOP 1 forced
				JsonValidator validator = new GenericValidator(parallelism, 9);
				TestingExecutionEnvironment.setAsNext(validator, parallelism);

				ConnectedComponents.main();
			}
			catch (AbortError ignored) {}

			// with arguments
			try {
				final int parallelism = 23;
				JsonValidator validator = new GenericValidator(parallelism, 9);
				TestingExecutionEnvironment.setAsNext(validator, parallelism);

				String tmpDir = tempFolder.newFolder().getAbsolutePath();
				ConnectedComponents.main(
						"--vertices", tmpDir,
						"--edges", tmpDir,
						"--output", tmpDir,
						"--iterations", "100");
			}
			catch (AbortError ignored) {}

		}
		catch (Exception e) {
			restoreStreams();
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------

	private interface JsonValidator {

		void validateJson(String json) throws Exception;
	}

	private static class GenericValidator implements JsonValidator {

		private final int expectedParallelism;
		private final int numNodes;

		GenericValidator(int expectedParallelism, int numNodes) {
			this.expectedParallelism = expectedParallelism;
			this.numNodes = numNodes;
		}

		@Override
		public void validateJson(String json) throws Exception {
			final Map<String, JsonNode> idToNode = new HashMap<>();

			// validate the produced JSON
			ObjectMapper m = new ObjectMapper();
			JsonNode rootNode = m.readTree(json);

			JsonNode idField = rootNode.get("jid");
			JsonNode nameField = rootNode.get("name");
			JsonNode arrayField = rootNode.get("nodes");

			assertNotNull(idField);
			assertNotNull(nameField);
			assertNotNull(arrayField);
			assertTrue(idField.isTextual());
			assertTrue(nameField.isTextual());
			assertTrue(arrayField.isArray());

			ArrayNode array = (ArrayNode) arrayField;
			Iterator<JsonNode> iter = array.elements();
			while (iter.hasNext()) {
				JsonNode vertex = iter.next();

				JsonNode vertexIdField = vertex.get("id");
				JsonNode parallelismField = vertex.get("parallelism");
				JsonNode contentsFields = vertex.get("description");
				JsonNode operatorField = vertex.get("operator");

				assertNotNull(vertexIdField);
				assertTrue(vertexIdField.isTextual());
				assertNotNull(parallelismField);
				assertTrue(parallelismField.isNumber());
				assertNotNull(contentsFields);
				assertTrue(contentsFields.isTextual());
				assertNotNull(operatorField);
				assertTrue(operatorField.isTextual());

				if (contentsFields.asText().startsWith("Sync")) {
					assertEquals(1, parallelismField.asInt());
				}
				else {
					assertEquals(expectedParallelism, parallelismField.asInt());
				}

				idToNode.put(vertexIdField.asText(), vertex);
			}

			assertEquals(numNodes, idToNode.size());

			// check that all inputs are contained
			for (JsonNode node : idToNode.values()) {
				JsonNode inputsField = node.get("inputs");
				if (inputsField != null) {
					Iterator<JsonNode> inputsIter = inputsField.elements();
					while (inputsIter.hasNext()) {
						JsonNode inputNode = inputsIter.next();
						JsonNode inputIdField = inputNode.get("id");

						assertNotNull(inputIdField);
						assertTrue(inputIdField.isTextual());

						String inputIdString = inputIdField.asText();
						assertTrue(idToNode.containsKey(inputIdString));
					}
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	private static class AbortError extends Error {
		private static final long serialVersionUID = 152179957828703919L;
	}

	// ------------------------------------------------------------------------

	private static class TestingExecutionEnvironment extends ExecutionEnvironment {

		private final JsonValidator validator;

		private TestingExecutionEnvironment(JsonValidator validator) {
			this.validator = validator;
		}

		@Override
		public void startNewSession() throws Exception {
		}

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			Plan plan = createProgramPlan(jobName);

			Optimizer pc = new Optimizer(new Configuration());
			OptimizedPlan op = pc.compile(plan);

			JobGraphGenerator jgg = new JobGraphGenerator();
			JobGraph jobGraph = jgg.compileJobGraph(op);

			String jsonPlan = JsonPlanGenerator.generatePlan(jobGraph);

			// first check that the JSON is valid
			JsonParser parser = new JsonFactory().createJsonParser(jsonPlan);
			while (parser.nextToken() != null) {}

			validator.validateJson(jsonPlan);

			throw new AbortError();
		}

		@Override
		public String getExecutionPlan() throws Exception {
			throw new UnsupportedOperationException();
		}

		public static void setAsNext(final JsonValidator validator, final int defaultParallelism) {
			initializeContextEnvironment(new ExecutionEnvironmentFactory() {
				@Override
				public ExecutionEnvironment createExecutionEnvironment() {
					ExecutionEnvironment env = new TestingExecutionEnvironment(validator);
					env.setParallelism(defaultParallelism);
					return env;
				}
			});
		}
	}
}

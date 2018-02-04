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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.legacy.AbstractJsonRequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.runtime.webmonitor.WebRuntimeMonitor;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Abstract handler for fetching plan for a jar or running a jar.
 */
public abstract class JarActionHandler extends AbstractJsonRequestHandler {

	private final File jarDir;

	public JarActionHandler(Executor executor, File jarDirectory) {
		super(executor);
		jarDir = jarDirectory;
	}

	protected Tuple2<JobGraph, ClassLoader> getJobGraphAndClassLoader(JarActionHandlerConfig config) throws Exception {
		// generate the graph
		JobGraph graph = null;

		if (!jarDir.exists()) {
			WebRuntimeMonitor.logExternalUploadDirDeletion(jarDir);
			try {
				WebRuntimeMonitor.checkAndCreateUploadDir(jarDir);
			} catch (IOException ioe) {
				// the following code will throw an exception since the jar can't be found
			}
		}

		PackagedProgram program = new PackagedProgram(
				new File(jarDir, config.getJarFile()),
				config.getEntryClass(),
				config.getProgramArgs());
		ClassLoader classLoader = program.getUserCodeClassLoader();

		Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), new Configuration());
		FlinkPlan plan = ClusterClient.getOptimizedPlan(optimizer, program, config.getParallelism());

		if (plan instanceof StreamingPlan) {
			graph = ((StreamingPlan) plan).getJobGraph();
		} else if (plan instanceof OptimizedPlan) {
			graph = new JobGraphGenerator().compileJobGraph((OptimizedPlan) plan);
		}
		if (graph == null) {
			throw new CompilerException("A valid job graph couldn't be generated for the jar.");
		}

		// Set the savepoint settings
		graph.setSavepointRestoreSettings(config.getSavepointRestoreSettings());

		for (URL jar : program.getAllLibraries()) {
			try {
				graph.addJar(new Path(jar.toURI()));
			}
			catch (URISyntaxException e) {
				throw new ProgramInvocationException("Invalid jar path. Unexpected error. :(");
			}
		}
		return Tuple2.of(graph, classLoader);
	}

	protected String sendError(Exception e) throws Exception {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartObject();
		gen.writeStringField("error", ExceptionUtils.stringifyException(e));
		gen.writeEndObject();
		gen.close();

		return writer.toString();
	}

	/**
	 * Wrapper for all configuration that is parsed from query and path args.
	 */
	@VisibleForTesting
	static class JarActionHandlerConfig {

		private final String jarFile;
		private final String[] programArgs;
		private final String entryClass;
		private final int parallelism;
		private final SavepointRestoreSettings savepointRestoreSettings;

		JarActionHandlerConfig(
				String jarFile,
				String[] programArgs,
				String entryClass,
				int parallelism,
				SavepointRestoreSettings savepointRestoreSettings) {

			this.jarFile = jarFile;
			this.programArgs = programArgs;
			this.entryClass = entryClass;
			this.parallelism = parallelism;
			this.savepointRestoreSettings = savepointRestoreSettings;
		}

		public String getJarFile() {
			return jarFile;
		}

		public String[] getProgramArgs() {
			return programArgs;
		}

		public String getEntryClass() {
			return entryClass;
		}

		public int getParallelism() {
			return parallelism;
		}

		public SavepointRestoreSettings getSavepointRestoreSettings() {
			return savepointRestoreSettings;
		}

		public static JarActionHandlerConfig fromParams(Map<String, String> pathParams, Map<String, String> queryParams) {
			// Jar file
			String jarFile = pathParams.get("jarid");
			if (jarFile == null) {
				throw new IllegalArgumentException("No jarid was provided.");
			}

			// Program args
			String[] programArgs = new String[0];
			String programArgsOpt = queryParams.get("program-args");
			if (programArgsOpt != null && !programArgsOpt.equals("")) {
				List<String> args = tokenizeArguments(programArgsOpt);
				programArgs = args.toArray(new String[args.size()]);
			}

			// Entry class
			String entryClass = null;
			String entryClassOpt = queryParams.get("entry-class");
			if (entryClassOpt != null && !entryClassOpt.equals("")) {
				entryClass = entryClassOpt;
			}

			// Parallelism
			int parallelism = 1;
			String parallelismOpt = queryParams.get("parallelism");
			if (parallelismOpt != null && !parallelismOpt.equals("")) {
				parallelism = Integer.parseInt(parallelismOpt);
				if (parallelism < 1) {
					throw new IllegalArgumentException("Parallelism must be a positive number.");
				}
			}

			// Savepoint restore settings
			SavepointRestoreSettings savepointSettings = SavepointRestoreSettings.none();
			String savepointPath = queryParams.get("savepointPath");
			if (savepointPath != null && !savepointPath.equals("")) {
				String allowNonRestoredOpt = queryParams.get("allowNonRestoredState");
				boolean allowNonRestoredState = allowNonRestoredOpt != null && allowNonRestoredOpt.equals("true");
				savepointSettings = SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState);
			}

			return new JarActionHandlerConfig(jarFile, programArgs, entryClass, parallelism, savepointSettings);
		}

		/**
		 * Utility method that takes the given arguments, splits them at the whitespaces (space and tab) and
		 * turns them into an array of Strings. Other than the <tt>StringTokenizer</tt>, this method
		 * takes care of quotes, such that quoted passages end up being one string.
		 *
		 * @param args
		 *        The string to be split.
		 * @return The array of split strings.
		 */
		private static List<String> tokenizeArguments(String args) {
			List<String> list = new ArrayList<String>();
			StringBuilder curr = new StringBuilder();

			int pos = 0;
			boolean quoted = false;

			while (pos < args.length()) {
				char c = args.charAt(pos);
				if ((c == ' ' || c == '\t') && !quoted) {
					if (curr.length() > 0) {
						list.add(curr.toString());
						curr.setLength(0);
					}
				} else if (c == '"') {
					quoted = !quoted;
				} else {
					curr.append(c);
				}

				pos++;
			}

			if (quoted) {
				throw new IllegalArgumentException("Unterminated quoted string.");
			}

			if (curr.length() > 0) {
				list.add(curr.toString());
			}

			return list;
		}
	}

}

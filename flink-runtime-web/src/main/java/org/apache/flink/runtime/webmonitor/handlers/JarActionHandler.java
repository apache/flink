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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.Client;
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

import java.io.File;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract handler for fetching plan for a jar or running a jar.
 */
public abstract class JarActionHandler implements RequestHandler, RequestHandler.JsonResponse {

	private final File jarDir;

	private static final PrintStream nullStream = new PrintStream(new NullPrintStream());

	public JarActionHandler(File jarDirectory) {
		jarDir = jarDirectory;
	}

	protected Tuple2<JobGraph, ClassLoader> getJobGraphAndClassLoader(Map<String, String> pathParams, Map<String, String> queryParams) throws Exception {
		// generate the graph
		JobGraph graph = null;
		final String file = pathParams.get("jarid");
		if (file == null) {
			throw new IllegalArgumentException("No jarid was provided.");
		}

		final List<String> programArgs;
		// parse required params
		String param = queryParams.get("program-args");
		programArgs = (param != null && !param.equals("")) ? tokenizeArguments(param) : new ArrayList<String>();

		final String entryClassOpt = queryParams.get("entry-class");
		final String parallelismOpt = queryParams.get("parallelism");

		int parallelism = 1;
		String entryClass = null;

		if (parallelismOpt != null && !parallelismOpt.equals("")) {
			parallelism = Integer.parseInt(parallelismOpt);
			if (parallelism < 1) {
				throw new IllegalArgumentException("Parallelism must be a positive number.");
			}
		}

		// get entry class
		if (entryClassOpt != null && !entryClassOpt.equals("")) {
			entryClass = entryClassOpt;
		}

		PackagedProgram program = new PackagedProgram(new File(jarDir, file), entryClass,
				programArgs.toArray(new String[programArgs.size()]));
		ClassLoader classLoader = program.getUserCodeClassLoader();
		Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), new Configuration());
		PrintStream out = System.out;
		PrintStream err = System.err;
		System.setOut(nullStream);
		System.setErr(nullStream);
		FlinkPlan plan = Client.getOptimizedPlan(compiler, program, parallelism);
		System.setOut(out);
		System.setErr(err);
		if (plan instanceof StreamingPlan) {
			graph = ((StreamingPlan) plan).getJobGraph();
		} else if (plan instanceof OptimizedPlan) {
			graph = new JobGraphGenerator().compileJobGraph((OptimizedPlan) plan);
		}
		if (graph == null) {
			throw new CompilerException("A valid job graph couldn't be generated for the jar.");
		}
		for (URL jar : program.getAllLibraries()) {
			try {
				graph.addJar(new Path(jar.toURI()));
			} catch (URISyntaxException e) {
				throw new ProgramInvocationException("Invalid jar path. Unexpected error. :(");
			}
		}
		return Tuple2.of(graph, classLoader);
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

	protected String sendError(Exception e) throws Exception {
		StringWriter sw = new StringWriter();
		PrintWriter p = new PrintWriter(sw);
		if (e instanceof ProgramInvocationException || e instanceof CompilerException || e instanceof IllegalArgumentException) {
			p.println(e.getClass().getSimpleName() + ((e.getMessage() != null) ? ": " + e.getMessage() : ""));
			Throwable cause = e.getCause();
			if (cause != null) {
				p.println(cause.toString());
			} else {
				cause = e;
			}

			for (StackTraceElement traceElement: cause.getStackTrace()) {
				p.println("\tat " + traceElement);
				if (traceElement.getMethodName().equals("handleRequest")) {
					break;
				}
			}
		} else {
			// if not something we expected, dump the entire stack trace.
			e.printStackTrace(p);
		}
		p.close();
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);
		gen.writeStartObject();
		gen.writeStringField("error", sw.toString());
		gen.writeEndObject();
		gen.close();
		return writer.toString();
	}

	private static final class NullPrintStream extends OutputStream {
		public void write(int x) {
		}
	}
}

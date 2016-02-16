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
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

/**
 * This handler handles requests to fetch plan for a jar.
 */
public class JarRunHandler extends JarActionHandler {

	private final FiniteDuration timeout;

	public JarRunHandler(File jarDirectory, FiniteDuration timeout) {
		super(jarDirectory);
		this.timeout = timeout;
	}

	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		try {
			Tuple2<JobGraph, ClassLoader> graph = getJobGraphAndClassLoader(pathParams, queryParams);
			try {
				JobClient.uploadJarFiles(graph.f0, jobManager, timeout);
			} catch (IOException e) {
				throw new ProgramInvocationException("Failed to upload jar files to the job manager", e);
			}

			try {
				JobClient.submitJobDetached(jobManager, graph.f0, timeout, graph.f1);
			} catch (JobExecutionException e) {
				throw new ProgramInvocationException("Failed to submit the job to the job manager", e);
			}

			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);
			gen.writeStartObject();
			gen.writeStringField("jobid", graph.f0.getJobID().toString());
			gen.writeEndObject();
			gen.close();
			return writer.toString();
		} catch (Exception e) {
			return sendError(e);
		}
	}
}

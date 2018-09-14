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

package org.apache.flink.runtime.webmonitor.handlers.legacy;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.JsonFactory;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.File;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * This handler handles requests to fetch plan for a jar.
 */
public class JarRunHandler extends JarActionHandler {

	static final String JAR_RUN_REST_PATH = "/jars/:jarid/run";

	private final Time timeout;
	private final Configuration clientConfig;

	public JarRunHandler(Executor executor, File jarDirectory, Time timeout, Configuration clientConfig) {
		super(executor, jarDirectory);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.clientConfig = Preconditions.checkNotNull(clientConfig);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JAR_RUN_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					JarActionHandlerConfig config = JarActionHandlerConfig.fromParams(pathParams, queryParams);
					Tuple2<JobGraph, ClassLoader> graph = getJobGraphAndClassLoader(config);

					try {
						JobClient.submitJobDetached(
							jobManagerGateway,
							clientConfig,
							graph.f0,
							timeout,
							graph.f1);
					} catch (JobExecutionException e) {
						throw new ProgramInvocationException("Failed to submit the job to the job manager", e);
					}

					StringWriter writer = new StringWriter();
					JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);
					gen.writeStartObject();
					gen.writeStringField("jobid", graph.f0.getJobID().toString());
					gen.writeEndObject();
					gen.close();
					return writer.toString();
				} catch (Exception e) {
					throw new CompletionException(new FlinkException("Could not run the jar.", e));
				}
			},
			executor);
	}
}

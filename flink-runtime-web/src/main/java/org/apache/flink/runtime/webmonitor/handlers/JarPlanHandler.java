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
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;

import java.io.File;
import java.io.StringWriter;
import java.util.Map;

/**
 * This handler handles requests to fetch plan for a jar.
 */
public class JarPlanHandler extends JarActionHandler {

	public JarPlanHandler(File jarDirectory) {
		super(jarDirectory);
	}

	@Override
	public String handleRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		try {
			JobGraph graph = getJobGraphAndClassLoader(pathParams, queryParams).f0;
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);
			gen.writeStartObject();
			gen.writeFieldName("plan");
			gen.writeRawValue(JsonPlanGenerator.generatePlan(graph));
			gen.writeEndObject();
			gen.close();
			return writer.toString();
		} catch (Exception e) {
			return sendError(e);
		}
	}
}

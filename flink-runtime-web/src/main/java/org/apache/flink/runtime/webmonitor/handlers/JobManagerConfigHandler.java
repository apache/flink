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
import org.apache.flink.configuration.Configuration;

import java.io.StringWriter;
import java.util.Map;

/**
 * Returns the Job Manager's configuration.
 */
public class JobManagerConfigHandler implements RequestHandler, RequestHandler.JsonResponse {

	private final Configuration config;

	public JobManagerConfigHandler(Configuration config) {
		this.config = config;
	}

	@Override
	public String handleRequest(Map<String, String> params) throws Exception {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createJsonGenerator(writer);

		gen.writeStartArray();
		for (String key : config.keySet()) {
			gen.writeStartObject();
			gen.writeStringField("key", key);
			gen.writeStringField("value", config.getString(key, null));
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.close();
		return writer.toString();
	}
}

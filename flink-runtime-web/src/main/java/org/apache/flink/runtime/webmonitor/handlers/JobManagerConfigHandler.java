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
import org.apache.flink.runtime.instance.ActorGateway;

import java.io.StringWriter;
import java.util.Map;

/**
 * Returns the Job Manager's configuration.
 */
public class JobManagerConfigHandler extends AbstractJsonRequestHandler {

	private static final String JOBMANAGER_CONFIG_REST_PATH = "/jobmanager/config";

	private final Configuration config;

	public JobManagerConfigHandler(Configuration config) {
		this.config = config;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOBMANAGER_CONFIG_REST_PATH};
	}

	@Override
	public String handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

		gen.writeStartArray();
		for (String key : config.keySet()) {
			gen.writeStartObject();
			gen.writeStringField("key", key);

			// Mask key values which contain sensitive information
			if(key.toLowerCase().contains("password")) {
				String value = config.getString(key, null);
				if(value != null) {
					value = "******";
				}
				gen.writeStringField("value", value);
			}
			else {
				gen.writeStringField("value", config.getString(key, null));
			}
			gen.writeEndObject();
		}
		gen.writeEndArray();

		gen.close();
		return writer.toString();
	}
}

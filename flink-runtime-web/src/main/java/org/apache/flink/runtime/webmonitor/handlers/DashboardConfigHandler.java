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
import org.apache.flink.runtime.webmonitor.utils.JsonUtils;

import java.io.StringWriter;
import java.util.Map;

/**
 * Responder that returns the parameters that define how the asynchronous requests
 * against this web server should behave. It defines for example the refresh interval,
 * and time zone of the server timestamps.
 */
public class DashboardConfigHandler extends AbstractJsonRequestHandler {
	
	private final String configString;
	
	public DashboardConfigHandler(long refreshInterval) {
		try {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);

			JsonUtils.writeConfigAsJson(gen, refreshInterval);
	
			gen.close();
			this.configString = writer.toString();
		}
		catch (Exception e) {
			// should never happen
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	@Override
	public String handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, ActorGateway jobManager) throws Exception {
		return this.configString;
	}
}

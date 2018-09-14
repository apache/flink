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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Responder that returns the parameters that define how the asynchronous requests
 * against this web server should behave. It defines for example the refresh interval,
 * and time zone of the server timestamps.
 */
public class DashboardConfigHandler extends AbstractJsonRequestHandler {

	public static final String DASHBOARD_CONFIG_REST_PATH = "/config";

	private final String configString;

	private final DashboardConfiguration dashboardConfiguration;

	public DashboardConfigHandler(Executor executor, long refreshInterval) {
		super(executor);

		dashboardConfiguration = DashboardConfiguration.from(refreshInterval, ZonedDateTime.now());

		try {
			this.configString = createConfigJson(dashboardConfiguration);
		}
		catch (Exception e) {
			// should never happen
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public String[] getPaths() {
		return new String[]{DASHBOARD_CONFIG_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		return CompletableFuture.completedFuture(configString);
	}

	public static String createConfigJson(DashboardConfiguration dashboardConfiguration) throws IOException {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

		gen.writeStartObject();
		gen.writeNumberField(DashboardConfiguration.FIELD_NAME_REFRESH_INTERVAL, dashboardConfiguration.getRefreshInterval());
		gen.writeNumberField(DashboardConfiguration.FIELD_NAME_TIMEZONE_OFFSET, dashboardConfiguration.getTimeZoneOffset());
		gen.writeStringField(DashboardConfiguration.FIELD_NAME_TIMEZONE_NAME, dashboardConfiguration.getTimeZoneName());
		gen.writeStringField(DashboardConfiguration.FIELD_NAME_FLINK_VERSION, dashboardConfiguration.getFlinkVersion());
		gen.writeStringField(DashboardConfiguration.FIELD_NAME_FLINK_REVISION, dashboardConfiguration.getFlinkRevision());

		gen.writeEndObject();

		gen.close();

		return writer.toString();
	}
}

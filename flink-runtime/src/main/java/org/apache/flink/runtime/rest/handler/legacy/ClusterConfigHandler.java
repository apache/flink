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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoEntry;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Returns the Job Manager's configuration.
 */
public class ClusterConfigHandler extends AbstractJsonRequestHandler {

	private final String clusterConfigJson;

	public ClusterConfigHandler(Executor executor, Configuration config) {
		super(executor);

		Preconditions.checkNotNull(config);
		this.clusterConfigJson = createConfigJson(config);
	}

	@Override
	public String[] getPaths() {
		return new String[]{ClusterConfigurationInfoHeaders.CLUSTER_CONFIG_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(
			Map<String, String> pathParams,
			Map<String, String> queryParams,
			JobManagerGateway jobManagerGateway) {

		return CompletableFuture.completedFuture(clusterConfigJson);
	}

	private static String createConfigJson(Configuration config) {
		try {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

			gen.writeStartArray();
			for (String key : config.keySet()) {
				gen.writeStartObject();
				gen.writeStringField(ClusterConfigurationInfoEntry.FIELD_NAME_CONFIG_KEY, key);

				String value = config.getString(key, null);
				// Mask key values which contain sensitive information
				if (value != null && GlobalConfiguration.isSensitive(key)) {
					value = GlobalConfiguration.HIDDEN_CONTENT;
				}
				gen.writeStringField(ClusterConfigurationInfoEntry.FIELD_NAME_CONFIG_VALUE, value);

				gen.writeEndObject();
			}
			gen.writeEndArray();

			gen.close();
			return writer.toString();
		} catch (IOException e) {
			throw new CompletionException(new FlinkException("Could not write configuration.", e));
		}
	}
}

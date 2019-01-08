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

package org.apache.flink.table.client.config;

import org.apache.flink.client.cli.CliFrontendParser;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration of a Flink cluster deployment. This class parses the `deployment` part
 * in an environment file. In the future, we should keep the amount of properties here little and
 * forward most properties to Flink's CLI frontend properties directly.
 */
public class Deployment {

	private final Map<String, String> properties;

	public Deployment() {
		this.properties = Collections.emptyMap();
	}

	private Deployment(Map<String, String> properties) {
		this.properties = properties;
	}

	public boolean isStandaloneDeployment() {
		return Objects.equals(
			properties.getOrDefault(PropertyStrings.DEPLOYMENT_TYPE, PropertyStrings.DEPLOYMENT_TYPE_VALUE_STANDALONE),
			PropertyStrings.DEPLOYMENT_TYPE_VALUE_STANDALONE);
	}

	public long getResponseTimeout() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.DEPLOYMENT_RESPONSE_TIMEOUT, Long.toString(10000)));
	}

	public String getGatewayAddress() {
		return properties.getOrDefault(PropertyStrings.DEPLOYMENT_GATEWAY_ADDRESS, "");
	}

	public int getGatewayPort() {
		return Integer.parseInt(properties.getOrDefault(PropertyStrings.DEPLOYMENT_GATEWAY_PORT, Integer.toString(0)));
	}

	/**
	 * Parses the given command line options from the deployment properties. Ignores properties
	 * that are not defined by options.
	 */
	public CommandLine getCommandLine(Options commandLineOptions) throws Exception {
		final List<String> args = new ArrayList<>();

		properties.forEach((k, v) -> {
			// only add supported options
			if (commandLineOptions.hasOption(k)) {
				final Option o = commandLineOptions.getOption(k);
				final String argument = "--" + o.getLongOpt();
				// options without args
				if (!o.hasArg()) {
					final Boolean flag = Boolean.parseBoolean(v);
					// add key only
					if (flag) {
						args.add(argument);
					}
				}
				// add key and value
				else if (!o.hasArgs()) {
					args.add(argument);
					args.add(v);
				}
				// options with multiple args are not supported yet
				else {
					throw new IllegalArgumentException("Option '" + o + "' is not supported yet.");
				}
			}
		});

		return CliFrontendParser.parse(commandLineOptions, args.toArray(new String[args.size()]), true);
	}

	public Map<String, String> toProperties() {
		final Map<String, String> copy = new HashMap<>();
		properties.forEach((k, v) -> copy.put(PropertyStrings.DEPLOYMENT + "." + k, v));
		return copy;
	}

	// --------------------------------------------------------------------------------------------

	public static Deployment create(Map<String, Object> config) {
		return new Deployment(ConfigUtil.normalizeYaml(config));
	}

	/**
	 * Merges two deployments. The properties of the first deployment might be overwritten by the second one.
	 */
	public static Deployment merge(Deployment deploy1, Deployment deploy2) {
		final Map<String, String> properties = new HashMap<>(deploy1.properties);
		properties.putAll(deploy2.properties);

		return new Deployment(properties);
	}

	/**
	 * Creates a new deployment enriched with additional properties.
	 */
	public static Deployment enrich(Deployment deploy, Map<String, String> properties) {
		final Map<String, String> newProperties = new HashMap<>(deploy.properties);
		properties.forEach((k, v) -> {
			final String normalizedKey = k.toLowerCase();
			if (k.startsWith(PropertyStrings.DEPLOYMENT + ".")) {
				newProperties.put(normalizedKey.substring(PropertyStrings.DEPLOYMENT.length() + 1), v);
			}
		});

		return new Deployment(newProperties);
	}
}

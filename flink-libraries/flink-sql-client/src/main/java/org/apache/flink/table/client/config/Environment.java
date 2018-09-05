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

import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.TableDescriptor;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.client.config.ConfigUtil.extractEarlyStringProperty;

/**
 * Environment configuration that represents the content of an environment file. Environment files
 * define tables, execution, and deployment behavior. An environment might be defined by default or
 * as part of a session. Environments can be merged or enriched with properties (e.g. from CLI command).
 *
 * <p>In future versions, we might restrict the merging or enrichment of deployment properties to not
 * allow overwriting of a deployment by a session.
 */
public class Environment {

	private Map<String, TableDescriptor> tables;

	private Map<String, String> views;

	private Map<String, UserDefinedFunction> functions;

	private Execution execution;

	private Deployment deployment;

	private static final String TABLE_NAME = "name";
	private static final String TABLE_TYPE = "type";
	private static final String TABLE_TYPE_VALUE_SOURCE = "source";
	private static final String TABLE_TYPE_VALUE_SINK = "sink";
	private static final String TABLE_TYPE_VALUE_BOTH = "both";
	private static final String VIEW_NAME = "name";
	private static final String VIEW_QUERY = "query";

	public Environment() {
		this.tables = Collections.emptyMap();
		this.views = Collections.emptyMap();
		this.functions = Collections.emptyMap();
		this.execution = new Execution();
		this.deployment = new Deployment();
	}

	public Map<String, TableDescriptor> getTables() {
		return tables;
	}

	public void setTables(List<Map<String, Object>> tables) {
		this.tables = new HashMap<>(tables.size());
		tables.forEach(config -> {
			final String name = extractEarlyStringProperty(config, TABLE_NAME, "table");
			final Map<String, Object> properties = new HashMap<>(config);
			properties.remove(TABLE_NAME);

			if (this.tables.containsKey(name) || this.views.containsKey(name)) {
				throw new SqlClientException("Cannot create table '" + name + "' because a table or " +
					"view with this name is already registered.");
			}
			this.tables.put(name, createTableDescriptor(name, properties));
		});
	}

	public Map<String, String> getViews() {
		return views;
	}

	public void setViews(List<Map<String, Object>> views) {
		// the order of how views are registered matters because
		// they might reference each other
		this.views = new LinkedHashMap<>(views.size());
		views.forEach(config -> {
			final String name = extractEarlyStringProperty(config, VIEW_NAME, "view");
			final String query = extractEarlyStringProperty(config, VIEW_QUERY, "view");

			if (this.tables.containsKey(name) || this.views.containsKey(name)) {
				throw new SqlClientException("Cannot create view '" + name + "' because a table or " +
					"view with this name is already registered.");
			}
			this.views.put(name, query);
		});
	}

	public Map<String, UserDefinedFunction> getFunctions() {
		return functions;
	}

	public void setFunctions(List<Map<String, Object>> functions) {
		this.functions = new HashMap<>(functions.size());
		functions.forEach(config -> {
			final UserDefinedFunction f = UserDefinedFunction.create(config);
			if (this.tables.containsKey(f.getName())) {
				throw new SqlClientException("Duplicate function name '" + f.getName() + "'.");
			}
			this.functions.put(f.getName(), f);
		});
	}

	public void setExecution(Map<String, Object> config) {
		this.execution = Execution.create(config);
	}

	public Execution getExecution() {
		return execution;
	}

	public void setDeployment(Map<String, Object> config) {
		this.deployment = Deployment.create(config);
	}

	public Deployment getDeployment() {
		return deployment;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("===================== Tables =====================\n");
		tables.forEach((name, table) -> {
			sb.append("- name: ").append(name).append("\n");
			final DescriptorProperties props = new DescriptorProperties(true);
			table.addProperties(props);
			props.asMap().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
		});
		sb.append("===================== Views =====================\n");
		views.forEach((name, query) -> {
			sb.append("- name: ").append(name).append("\n");
			sb.append("  ").append(VIEW_QUERY).append(": ").append(query).append('\n');
		});
		sb.append("=================== Functions ====================\n");
		functions.forEach((name, function) -> {
			sb.append("- name: ").append(name).append("\n");
			final DescriptorProperties props = new DescriptorProperties(true);
			function.addProperties(props);
			props.asMap().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
		});
		sb.append("=================== Execution ====================\n");
		execution.toProperties().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
		sb.append("=================== Deployment ===================\n");
		deployment.toProperties().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
		return sb.toString();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Parses an environment file from an URL.
	 */
	public static Environment parse(URL url) throws IOException {
		return new ConfigUtil.LowerCaseYamlMapper().readValue(url, Environment.class);
	}

	/**
	 * Parses an environment file from an String.
	 */
	public static Environment parse(String content) throws IOException {
		return new ConfigUtil.LowerCaseYamlMapper().readValue(content, Environment.class);
	}

	/**
	 * Merges two environments. The properties of the first environment might be overwritten by the second one.
	 */
	public static Environment merge(Environment env1, Environment env2) {
		final Environment mergedEnv = new Environment();

		// merge tables
		final Map<String, TableDescriptor> tables = new HashMap<>(env1.getTables());
		tables.putAll(env2.getTables());
		mergedEnv.tables = tables;

		// merge views
		final LinkedHashMap<String, String> views = new LinkedHashMap<>(env1.getViews());
		views.putAll(env2.getViews());
		mergedEnv.views = views;

		// merge functions
		final Map<String, UserDefinedFunction> functions = new HashMap<>(env1.getFunctions());
		functions.putAll(env2.getFunctions());
		mergedEnv.functions = functions;

		// merge execution properties
		mergedEnv.execution = Execution.merge(env1.getExecution(), env2.getExecution());

		// merge deployment properties
		mergedEnv.deployment = Deployment.merge(env1.getDeployment(), env2.getDeployment());

		return mergedEnv;
	}

	/**
	 * Enriches an environment with new/modified properties or views and returns the new instance.
	 */
	public static Environment enrich(
			Environment env,
			Map<String, String> properties,
			Map<String, String> views) {
		final Environment enrichedEnv = new Environment();

		// merge tables
		enrichedEnv.tables = new HashMap<>(env.getTables());

		// merge functions
		enrichedEnv.functions = new HashMap<>(env.getFunctions());

		// enrich execution properties
		enrichedEnv.execution = Execution.enrich(env.execution, properties);

		// enrich deployment properties
		enrichedEnv.deployment = Deployment.enrich(env.deployment, properties);

		// enrich views
		enrichedEnv.views = new LinkedHashMap<>(env.getViews());
		enrichedEnv.views.putAll(views);

		return enrichedEnv;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a table descriptor from a YAML config map.
	 *
	 * @param name name of the table
	 * @param config YAML config map
	 * @return table descriptor describing a source, sink, or both
	 */
	private static TableDescriptor createTableDescriptor(String name, Map<String, Object> config) {
		final Object typeObject = config.get(TABLE_TYPE);
		if (typeObject == null || !(typeObject instanceof String)) {
			throw new SqlClientException("Invalid 'type' attribute for table '" + name + "'.");
		}
		final String type = (String) typeObject;
		final Map<String, Object> configCopy = new HashMap<>(config);
		configCopy.remove(TABLE_TYPE);

		final Map<String, String> normalizedConfig = ConfigUtil.normalizeYaml(configCopy);
		switch (type) {
			case TABLE_TYPE_VALUE_SOURCE:
				return new Source(name, normalizedConfig);
			case TABLE_TYPE_VALUE_SINK:
				return new Sink(name, normalizedConfig);
			case TABLE_TYPE_VALUE_BOTH:
				return new SourceSink(name, normalizedConfig);
			default:
				throw new SqlClientException(String.format("Invalid 'type' attribute for table '%s'. " +
					"Only 'source', 'sink', and 'both' are supported. But was '%s'.", name, type));
		}
	}
}

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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines all parameters that initialize a table environment. Those parameters are used only
 * during instantiation of a {@link TableEnvironment} and cannot be changed afterwards.
 *
 * <p>Example:
 * <pre>{@code
 *    EnvironmentSettings.newInstance()
 *      .useOldPlanner()
 *      .inStreamingMode()
 *      .withBuiltInCatalogName("default_catalog")
 *      .withBuiltInDatabaseName("default_database")
 *      .build()
 * }</pre>
 */
@PublicEvolving
public class EnvironmentSettings {
	public static final String BATCH_MODE = "batch-mode";
	public static final String CLASS_NAME = "class-name";

	/**
	 * Canonical name of the {@link Planner} class to use.
	 */
	private final String plannerClass;

	/**
	 * Canonical name of the {@link Executor} class to use.
	 */
	private final String executorClass;

	/**
	 * Specifies the name of the initial catalog to be created when instantiating
	 * {@link TableEnvironment}.
	 */
	private final String builtInCatalogName;

	/**
	 * Specifies the name of the default database in the initial catalog to be created when
	 * instantiating {@link TableEnvironment}.
	 */
	private final String builtInDatabaseName;

	/**
	 * Determines if the table environment should work in a batch ({@code true}) or
	 * streaming ({@code false}) mode.
	 */
	private final boolean isBatchMode;

	private EnvironmentSettings(
			@Nullable String plannerClass,
			@Nullable String executorClass,
			String builtInCatalogName,
			String builtInDatabaseName,
			boolean isBatchMode) {
		this.plannerClass = plannerClass;
		this.executorClass = executorClass;
		this.builtInCatalogName = builtInCatalogName;
		this.builtInDatabaseName = builtInDatabaseName;
		this.isBatchMode = isBatchMode;
	}

	/**
	 * Creates a builder for creating an instance of {@link EnvironmentSettings}.
	 *
	 * <p>By default, it does not specify a required planner and will use the one that is available
	 * on the classpath via discovery.
	 */
	public static Builder newInstance() {
		return new Builder();
	}

	/**
	 * Gets the specified name of the initial catalog to be created when instantiating
	 * a {@link TableEnvironment}.
	 */
	public String getBuiltInCatalogName() {
		return builtInCatalogName;
	}

	/**
	 * Gets the specified name of the default database in the initial catalog to be created when instantiating
	 * a {@link TableEnvironment}.
	 */
	public String getBuiltInDatabaseName() {
		return builtInDatabaseName;
	}

	/**
	 * Tells if the {@link TableEnvironment} should work in a batch or streaming mode.
	 */
	public boolean isBatchMode() {
		return isBatchMode;
	}

	@Internal
	public Map<String, String> toPlannerProperties() {
		Map<String, String> properties = new HashMap<>(toCommonProperties());
		if (plannerClass != null) {
			properties.put(CLASS_NAME, plannerClass);
		}
		return properties;
	}

	@Internal
	public Map<String, String> toExecutorProperties() {
		Map<String, String> properties = new HashMap<>(toCommonProperties());
		if (executorClass != null) {
			properties.put(CLASS_NAME, executorClass);
		}
		return properties;
	}

	private Map<String, String> toCommonProperties() {
		Map<String, String> properties = new HashMap<>();
		properties.put(BATCH_MODE, Boolean.toString(isBatchMode));
		return properties;
	}

	/**
	 * A builder for {@link EnvironmentSettings}.
	 */
	public static class Builder {
		private String plannerClass = null;
		private String executorClass = null;
		private String builtInCatalogName = "default_catalog";
		private String builtInDatabaseName = "default_database";
		private boolean isBatchMode = false;

		/**
		 * Sets the old Flink planner as the required module. By default, {@link #useAnyPlanner()} is
		 * enabled.
		 */
		public Builder useOldPlanner() {
			this.plannerClass = "org.apache.flink.table.planner.StreamPlannerFactory";
			this.executorClass = "org.apache.flink.table.executor.StreamExecutorFactory";
			return this;
		}

		/**
		 * Sets the Blink planner as the required module. By default, {@link #useAnyPlanner()} is
		 * enabled.
		 */
		public Builder useBlinkPlanner() {
			this.plannerClass = "org.apache.flink.table.planner.BlinkPlannerFactory";
			this.executorClass = "org.apache.flink.table.executor.BlinkExecutorFactory";
			return this;
		}

		/**
		 * Does not set a planner requirement explicitly.
		 *
		 * <p>A planner will be discovered automatically, if there is only one planner available.
		 *
		 * <p>This is the default behavior.
		 */
		public Builder useAnyPlanner() {
			this.plannerClass = null;
			this.executorClass = null;
			return this;
		}

		/**
		 * Sets that the components should work in a batch mode. Streaming mode by default.
		 */
		public Builder inBatchMode() {
			this.isBatchMode = true;
			return this;
		}

		/**
		 * Sets that the components should work in a streaming mode. Enabled by default.
		 */
		public Builder inStreamingMode() {
			this.isBatchMode = false;
			return this;
		}

		/**
		 * Specifies the name of the initial catalog to be created when instantiating
		 * a {@link TableEnvironment}. Default: "default_catalog".
		 */
		public Builder withBuiltInCatalogName(String builtInCatalogName) {
			this.builtInCatalogName = builtInCatalogName;
			return this;
		}

		/**
		 * Specifies the name of the default database in the initial catalog to be created when instantiating
		 * a {@link TableEnvironment}. Default: "default_database".
		 */
		public Builder withBuiltInDatabaseName(String builtInDatabaseName) {
			this.builtInDatabaseName = builtInDatabaseName;
			return this;
		}

		/**
		 * Returns an immutable instance of {@link EnvironmentSettings}.
		 */
		public EnvironmentSettings build() {
			return new EnvironmentSettings(
				plannerClass,
				executorClass,
				builtInCatalogName,
				builtInDatabaseName,
				isBatchMode);
		}
	}
}

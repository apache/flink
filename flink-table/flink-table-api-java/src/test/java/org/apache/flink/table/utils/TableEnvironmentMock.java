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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.module.ModuleManager;

/**
 * Mocking {@link TableEnvironment} for tests.
 */
public class TableEnvironmentMock extends TableEnvironmentImpl {

	public final CatalogManager catalogManager;

	public final ExecutorMock executor;

	public final FunctionCatalog functionCatalog;

	public final PlannerMock planner;

	protected TableEnvironmentMock(
			CatalogManager catalogManager,
			ModuleManager moduleManager,
			TableConfig tableConfig,
			ExecutorMock executor,
			FunctionCatalog functionCatalog,
			PlannerMock planner,
			boolean isStreamingMode) {
		super(catalogManager, moduleManager, tableConfig, executor, functionCatalog, planner, isStreamingMode);

		this.catalogManager = catalogManager;
		this.executor = executor;
		this.functionCatalog = functionCatalog;
		this.planner = planner;
	}

	public static TableEnvironmentMock getStreamingInstance() {
		return getInstance(true);
	}

	public static TableEnvironmentMock getBatchInstance() {
		return getInstance(false);
	}

	private static TableEnvironmentMock getInstance(boolean isStreamingMode) {
		final TableConfig config = createTableConfig();
		final CatalogManager catalogManager = createCatalogManager();
		final ModuleManager moduleManager = new ModuleManager();
		return new TableEnvironmentMock(
			catalogManager,
			moduleManager,
			config,
			createExecutor(),
			createFunctionCatalog(config, catalogManager, moduleManager),
			createPlanner(),
			isStreamingMode);
	}

	private static CatalogManager createCatalogManager() {
		return new CatalogManager(
			EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
			new GenericInMemoryCatalog(
				EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
				EnvironmentSettings.DEFAULT_BUILTIN_DATABASE));
	}

	private static TableConfig createTableConfig() {
		return TableConfig.getDefault();
	}

	private static ExecutorMock createExecutor() {
		return new ExecutorMock();
	}

	private static FunctionCatalog createFunctionCatalog(
			TableConfig config,
			CatalogManager catalogManager,
			ModuleManager moduleManager) {
		return new FunctionCatalog(config, catalogManager, moduleManager);
	}

	private static PlannerMock createPlanner() {
		return new PlannerMock();
	}
}

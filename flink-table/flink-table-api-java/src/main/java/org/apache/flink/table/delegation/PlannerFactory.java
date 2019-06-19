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

package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.factories.ComponentFactory;

import java.util.Map;

/**
 * Factory that creates {@link Planner}.
 *
 * <p>This factory is used with Java's Service Provider Interfaces (SPI) for discovering. A factory is
 * called with a set of normalized properties that describe the desired configuration. Those properties
 * may include execution configurations such as watermark interval, max parallelism etc., table specific
 * initialization configuration such as if the queries should be executed in batch mode.
 */
@Internal
public interface PlannerFactory extends ComponentFactory {

	/**
	 * Creates a corresponding {@link Planner}.
	 *
	 * @param properties Static properties of the {@link Planner}, the same that were used for factory lookup.
	 * @param executor The executor required by the planner.
	 * @param tableConfig The configuration of the planner to use.
	 * @param functionCatalog The function catalog to look up user defined functions.
	 * @param catalogManager The catalog manager to look up tables and views.
	 * @return instance of a {@link Planner}
	 */
	Planner create(
		Map<String, String> properties,
		Executor executor,
		TableConfig tableConfig,
		FunctionCatalog functionCatalog,
		CatalogManager catalogManager);
}

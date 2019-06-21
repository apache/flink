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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;

import java.lang.reflect.Constructor;

/**
 * Factory to construct a {@link Planner}. It will look for the planner on the classpath.
 */
@Internal
public final class PlannerFactory {

	/**
	 * Looks up {@link Planner} on the class path via reflection.
	 *
	 * @param executor The executor required by the planner.
	 * @param tableConfig The configuration of the planner to use.
	 * @param functionCatalog The function catalog to look up user defined functions.
	 * @param catalogManager The catalog manager to look up tables and views.
	 * @return instance of a {@link Planner}
	 */
	public static Planner lookupPlanner(
			Executor executor,
			TableConfig tableConfig,
			FunctionCatalog functionCatalog,
			CatalogManager catalogManager) {
		try {
			Class<?> clazz = Class.forName("org.apache.flink.table.planner.StreamPlanner");
			Constructor con = clazz.getConstructor(
				Executor.class,
				TableConfig.class,
				FunctionCatalog.class,
				CatalogManager.class);

			return (Planner) con.newInstance(executor, tableConfig, functionCatalog, catalogManager);
		} catch (Exception e) {
			throw new TableException(
				"Could not instantiate the planner. Make sure the planner module is on the classpath",
				e);
		}
	}

	private PlannerFactory() {
	}
}

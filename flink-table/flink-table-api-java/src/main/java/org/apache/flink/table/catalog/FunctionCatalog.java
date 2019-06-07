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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.functions.UserFunctionsTypeHelper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Simple function catalog to store {@link FunctionDefinition}s in memory.
 */
@Internal
public class FunctionCatalog implements FunctionLookup {

	private final String defaultCatalogName;

	private final String defaultDatabaseName;

	private final Map<String, FunctionDefinition> userFunctions = new LinkedHashMap<>();

	public FunctionCatalog(String defaultCatalogName, String defaultDatabaseName) {
		this.defaultCatalogName = defaultCatalogName;
		this.defaultDatabaseName = defaultDatabaseName;
	}

	public void registerScalarFunction(String name, ScalarFunction function) {
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());
		registerFunction(
			name,
			new ScalarFunctionDefinition(name, function)
		);
	}

	public <T> void registerTableFunction(
			String name,
			TableFunction<T> function,
			TypeInformation<T> resultType) {
		// check if class not Scala object
		UserFunctionsTypeHelper.validateNotSingleton(function.getClass());
		// check if class could be instantiated
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());

		registerFunction(
			name,
			new TableFunctionDefinition(name, function, resultType)
		);
	}

	public <T, ACC> void registerAggregateFunction(
			String name,
			UserDefinedAggregateFunction<T, ACC> function,
			TypeInformation<T> resultType,
			TypeInformation<ACC> accType) {
		// check if class not Scala object
		UserFunctionsTypeHelper.validateNotSingleton(function.getClass());
		// check if class could be instantiated
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());

		registerFunction(
			name,
			new AggregateFunctionDefinition(name, function, resultType, accType)
		);
	}

	public String[] getUserDefinedFunctions() {
		return userFunctions.values().stream().map(FunctionDefinition::getName).toArray(String[]::new);
	}

	@Override
	public Optional<FunctionLookup.Result> lookupFunction(String name) {
		final FunctionDefinition userCandidate = userFunctions.get(normalizeName(name));
		final Optional<FunctionDefinition> foundDefinition;
		if (userCandidate != null) {
			foundDefinition = Optional.of(userCandidate);
		} else {
			foundDefinition = BuiltInFunctionDefinitions.getDefinitions()
				.stream()
				.filter(f -> normalizeName(name).equals(normalizeName(f.getName())))
				.findFirst();
		}

		return foundDefinition.map(definition -> new FunctionLookup.Result(
			ObjectIdentifier.of(defaultCatalogName, defaultDatabaseName, name),
			definition)
		);
	}

	private void registerFunction(String name, FunctionDefinition functionDefinition) {
		userFunctions.put(normalizeName(name), functionDefinition);
	}

	private String normalizeName(String name) {
		return name.toUpperCase();
	}
}

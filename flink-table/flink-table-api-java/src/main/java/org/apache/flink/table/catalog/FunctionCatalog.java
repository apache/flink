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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.functions.UserFunctionsTypeHelper;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple function catalog to store {@link FunctionDefinition}s in catalogs.
 */
@Internal
public class FunctionCatalog implements FunctionLookup {

	private final CatalogManager catalogManager;

	// For simplicity, currently hold registered Flink functions in memory here
	// TODO: should move to catalog
	private final Map<String, FunctionDefinition> userFunctions = new LinkedHashMap<>();

	/**
	 * Temporary utility until the new type inference is fully functional. It needs to be set by the planner.
	 */
	private PlannerTypeInferenceUtil plannerTypeInferenceUtil;

	public FunctionCatalog(CatalogManager catalogManager) {
		this.catalogManager = checkNotNull(catalogManager);
	}

	public void setPlannerTypeInferenceUtil(PlannerTypeInferenceUtil plannerTypeInferenceUtil) {
		this.plannerTypeInferenceUtil = plannerTypeInferenceUtil;
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
			new TableFunctionDefinition(
				name,
				function,
				resultType)
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

		final FunctionDefinition definition;
		if (function instanceof AggregateFunction) {
			definition = new AggregateFunctionDefinition(
				name,
				(AggregateFunction<?, ?>) function,
				resultType,
				accType);
		} else if (function instanceof TableAggregateFunction) {
			definition = new TableAggregateFunctionDefinition(
				name,
				(TableAggregateFunction<?, ?>) function,
				resultType,
				accType);
		} else {
			throw new TableException("Unknown function class: " + function.getClass());
		}

		registerFunction(
			name,
			definition
		);
	}

	public String[] getUserDefinedFunctions() {
		List<String> result = new ArrayList<>();

		// Get functions in catalog
		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
		try {
			result.addAll(catalog.listFunctions(catalogManager.getCurrentDatabase()));
		} catch (DatabaseNotExistException e) {
			// Ignore since there will always be a current database of the current catalog
		}

		// Get functions registered in memory
		result.addAll(
			userFunctions.values().stream()
				.map(FunctionDefinition::toString)
				.collect(Collectors.toList()));

		return result.toArray(new String[0]);
	}

	@Override
	public Optional<FunctionLookup.Result> lookupFunction(String name) {
		String functionName = normalizeName(name);

		FunctionDefinition userCandidate;

		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();

		try {
			CatalogFunction catalogFunction = catalog.getFunction(
				new ObjectPath(catalogManager.getCurrentDatabase(), functionName));

			if (catalog.getTableFactory().isPresent() &&
				catalog.getTableFactory().get() instanceof FunctionDefinitionFactory) {

				FunctionDefinitionFactory factory = (FunctionDefinitionFactory) catalog.getTableFactory().get();

				userCandidate = factory.createFunctionDefinition(functionName, catalogFunction);

				return Optional.of(
					new FunctionLookup.Result(
						ObjectIdentifier.of(catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase(), name),
						userCandidate)
				);
			} else {
				// TODO: should go through function definition discover service
			}
		} catch (FunctionNotExistException e) {
			// Ignore
		}

		// If no corresponding function is found in catalog, check in-memory functions
		userCandidate = userFunctions.get(functionName);

		final Optional<FunctionDefinition> foundDefinition;
		if (userCandidate != null) {
			foundDefinition = Optional.of(userCandidate);
		} else {

			// TODO once we connect this class with the Catalog APIs we need to make sure that
			//  built-in functions are present in "root" built-in catalog. This allows to
			//  overwrite built-in functions but also fallback to the "root" catalog. It should be
			//  possible to disable the "root" catalog if that is desired.

			foundDefinition = BuiltInFunctionDefinitions.getDefinitions()
				.stream()
				.filter(f -> functionName.equals(normalizeName(f.getName())))
				.findFirst()
				.map(Function.identity());
		}

		return foundDefinition.map(definition -> new FunctionLookup.Result(
			ObjectIdentifier.of(
				catalogManager.getBuiltInCatalogName(),
				catalogManager.getBuiltInDatabaseName(),
				name),
			definition)
		);
	}

	@Override
	public PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
		Preconditions.checkNotNull(
			plannerTypeInferenceUtil,
			"A planner should have set the type inference utility.");
		return plannerTypeInferenceUtil;
	}

	private void registerFunction(String name, FunctionDefinition functionDefinition) {
		// TODO: should register to catalog
		userFunctions.put(normalizeName(name), functionDefinition);
	}

	private String normalizeName(String name) {
		return name.toUpperCase();
	}
}

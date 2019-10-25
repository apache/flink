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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinitionUtil;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.functions.UserFunctionsTypeHelper;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple function catalog to store {@link FunctionDefinition}s in catalogs.
 */
@Internal
public class FunctionCatalog implements FunctionLookup {

	private final CatalogManager catalogManager;
	private final ModuleManager moduleManager;

	private final Map<String, FunctionDefinition> tempSystemFunctions = new LinkedHashMap<>();
	private final Map<ObjectIdentifier, FunctionDefinition> tempCatalogFunctions = new LinkedHashMap<>();

	/**
	 * Temporary utility until the new type inference is fully functional. It needs to be set by the planner.
	 */
	private PlannerTypeInferenceUtil plannerTypeInferenceUtil;

	public FunctionCatalog(CatalogManager catalogManager, ModuleManager moduleManager) {
		this.catalogManager = checkNotNull(catalogManager);
		this.moduleManager = checkNotNull(moduleManager);
	}

	public void setPlannerTypeInferenceUtil(PlannerTypeInferenceUtil plannerTypeInferenceUtil) {
		this.plannerTypeInferenceUtil = plannerTypeInferenceUtil;
	}

	public void registerTempSystemScalarFunction(String name, ScalarFunction function) {
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());
		registerTempSystemFunction(
			name,
			new ScalarFunctionDefinition(name, function)
		);
	}

	public <T> void registerTempSystemTableFunction(
			String name,
			TableFunction<T> function,
			TypeInformation<T> resultType) {
		// check if class not Scala object
		UserFunctionsTypeHelper.validateNotSingleton(function.getClass());
		// check if class could be instantiated
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());

		registerTempSystemFunction(
			name,
			new TableFunctionDefinition(
				name,
				function,
				resultType)
		);
	}

	public <T, ACC> void registerTempSystemAggregateFunction(
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

		registerTempSystemFunction(
			name,
			definition
		);
	}

	public void registerTempCatalogScalarFunction(ObjectIdentifier oi, ScalarFunction function) {
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());
		registerTempCatalogFunction(
			oi,
			new ScalarFunctionDefinition(oi.getObjectName(), function)
		);
	}

	public <T> void registerTempCatalogTableFunction(
			ObjectIdentifier oi,
			TableFunction<T> function,
			TypeInformation<T> resultType) {
		// check if class not Scala object
		UserFunctionsTypeHelper.validateNotSingleton(function.getClass());
		// check if class could be instantiated
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());

		registerTempCatalogFunction(
			oi,
			new TableFunctionDefinition(
				oi.getObjectName(),
				function,
				resultType)
		);
	}

	public <T, ACC> void registerTempCatalogAggregateFunction(
			ObjectIdentifier oi,
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
				oi.getObjectName(),
				(AggregateFunction<?, ?>) function,
				resultType,
				accType);
		} else if (function instanceof TableAggregateFunction) {
			definition = new TableAggregateFunctionDefinition(
				oi.getObjectName(),
				(TableAggregateFunction<?, ?>) function,
				resultType,
				accType);
		} else {
			throw new TableException("Unknown function class: " + function.getClass());
		}

		registerTempCatalogFunction(
			oi,
			definition
		);
	}

	public String[] getUserDefinedFunctions() {
		return getUserDefinedFunctionNames().toArray(new String[0]);
	}

	public String[] getFunctions() {
		Set<String> result = getUserDefinedFunctionNames();

		// Get built-in functions
		result.addAll(moduleManager.listFunctions());

		return result.toArray(new String[0]);
	}

	private Set<String> getUserDefinedFunctionNames() {
		Set<String> result = new HashSet<>();

		// Get functions in catalog
		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
		try {
			result.addAll(catalog.listFunctions(catalogManager.getCurrentDatabase()));
		} catch (DatabaseNotExistException e) {
			// Ignore since there will always be a current database of the current catalog
		}

		// Get functions registered in memory
		result.addAll(
			tempSystemFunctions.values().stream()
				.map(FunctionDefinition::toString)
				.collect(Collectors.toSet()));

		return result;
	}

	@Override
	public Optional<FunctionLookup.Result> lookupFunction(FunctionIdentifier fi) {

		// precise function reference
		if (fi.getIdentifier().isPresent()) {
			return resolvePreciseFunctionReference(fi.getIdentifier().get());
		} else {
			// ambiguous function reference

			String functionName = FunctionCatalogUtil.normalizeName(fi.getSimpleName().get());

			FunctionDefinition userCandidate;

			Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
			try {
				CatalogFunction catalogFunction = catalog.getFunction(
					new ObjectPath(catalogManager.getCurrentDatabase(), functionName)
				);

				if (catalog.getFunctionDefinitionFactory().isPresent()) {
					userCandidate = catalog.getFunctionDefinitionFactory().get().createFunctionDefinition(functionName, catalogFunction);
				} else {
					userCandidate = FunctionDefinitionUtil.createFunctionDefinition(functionName, catalogFunction);
				}

				return Optional.of(
					new FunctionLookup.Result(
						FunctionIdentifier.of(
							ObjectIdentifier.of(
								catalogManager.getCurrentCatalog(),
								catalogManager.getCurrentDatabase(),
								functionName)),
						userCandidate)
				);

			} catch (FunctionNotExistException e) {
				// ignore
			}

			// If no corresponding function is found in catalog, check in-memory functions
			userCandidate = tempSystemFunctions.get(functionName);

			final Optional<FunctionDefinition> foundDefinition;
			if (userCandidate != null) {
				foundDefinition = Optional.of(userCandidate);
			} else {
				foundDefinition = moduleManager.getFunctionDefinition(functionName);
			}

			return foundDefinition.map(d -> new FunctionLookup.Result(
				FunctionIdentifier.of(fi.getSimpleName().get()),
				d)
			);
		}
	}

	private Optional<FunctionLookup.Result> resolvePreciseFunctionReference(ObjectIdentifier oi) {
		// resolve order:
		// 1. Temporary functions
		// 2. Catalog functions
		ObjectIdentifier normalized = normalizeObjectIdentifier(oi);

		FunctionDefinition potentialResult = tempCatalogFunctions.get(normalized);

		if (potentialResult != null) {
			return Optional.of(
				new FunctionLookup.Result(
					FunctionIdentifier.of(oi),
					potentialResult
				)
			);
		}

		Catalog catalog = catalogManager.getCatalog(normalized.getCatalogName()).get();

		if (catalog != null) {
			try {
				CatalogFunction catalogFunction = catalog.getFunction(
					new ObjectPath(normalized.getDatabaseName(), normalized.getObjectName()));

				FunctionDefinition fd;
				if (catalog.getFunctionDefinitionFactory().isPresent()) {
					fd = catalog.getFunctionDefinitionFactory().get()
						.createFunctionDefinition(normalized.getObjectName(), catalogFunction);
				} else {
					fd = FunctionDefinitionUtil.createFunctionDefinition(normalized.getObjectName(), catalogFunction);
				}

				return Optional.of(
					new FunctionLookup.Result(FunctionIdentifier.of(oi), fd));
			} catch (FunctionNotExistException e) {
				// Ignore
			}
		}

		return Optional.empty();
	}

	@Override
	public PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
		Preconditions.checkNotNull(
			plannerTypeInferenceUtil,
			"A planner should have set the type inference utility.");
		return plannerTypeInferenceUtil;
	}

	private void registerTempSystemFunction(String name, FunctionDefinition functionDefinition) {
		tempSystemFunctions.put(FunctionCatalogUtil.normalizeName(name), functionDefinition);
	}

	private void registerTempCatalogFunction(ObjectIdentifier oi, FunctionDefinition functionDefinition) {
		tempCatalogFunctions.put(normalizeObjectIdentifier(oi), functionDefinition);
	}

	/**
	 * Only normalize the function name.
	 */
	@VisibleForTesting
	static ObjectIdentifier normalizeObjectIdentifier(ObjectIdentifier oi) {
		return ObjectIdentifier.of(
			oi.getCatalogName(),
			oi.getDatabaseName(),
			FunctionCatalogUtil.normalizeName(oi.getObjectName()));
	}
}

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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
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
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
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
	private final TableConfig config;
	private final CatalogManager catalogManager;
	private final ModuleManager moduleManager;

	private final Map<String, FunctionDefinition> tempSystemFunctions = new LinkedHashMap<>();
	private final Map<ObjectIdentifier, FunctionDefinition> tempCatalogFunctions = new LinkedHashMap<>();

	/**
	 * Temporary utility until the new type inference is fully functional. It needs to be set by the planner.
	 */
	private PlannerTypeInferenceUtil plannerTypeInferenceUtil;

	public FunctionCatalog(
			TableConfig config,
			CatalogManager catalogManager,
			ModuleManager moduleManager) {
		this.config = checkNotNull(config);
		this.catalogManager = checkNotNull(catalogManager);
		this.moduleManager = checkNotNull(moduleManager);
	}

	public void setPlannerTypeInferenceUtil(PlannerTypeInferenceUtil plannerTypeInferenceUtil) {
		this.plannerTypeInferenceUtil = plannerTypeInferenceUtil;
	}

	public void registerTempSystemScalarFunction(String name, ScalarFunction function) {
		UserDefinedFunctionHelper.prepareFunction(config, function);

		registerTempSystemFunction(
			name,
			new ScalarFunctionDefinition(name, function)
		);
	}

	public <T> void registerTempSystemTableFunction(
			String name,
			TableFunction<T> function,
			TypeInformation<T> resultType) {
		UserDefinedFunctionHelper.prepareFunction(config, function);

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
		UserDefinedFunctionHelper.prepareFunction(config, function);

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
		UserDefinedFunctionHelper.prepareFunction(config, function);

		registerTempCatalogFunction(
			oi,
			new ScalarFunctionDefinition(oi.getObjectName(), function)
		);
	}

	public <T> void registerTempCatalogTableFunction(
			ObjectIdentifier oi,
			TableFunction<T> function,
			TypeInformation<T> resultType) {
		UserDefinedFunctionHelper.prepareFunction(config, function);

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
		UserDefinedFunctionHelper.prepareFunction(config, function);

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

	/**
	 * Check whether a temporary catalog function is already registered.
	 * @param functionIdentifier the object identifier of function
	 * @return whether the temporary catalog function exists in the function catalog
	 */
	public boolean hasTemporaryCatalogFunction(ObjectIdentifier functionIdentifier) {
		ObjectIdentifier normalizedIdentifier =
			FunctionIdentifier.normalizeObjectIdentifier(functionIdentifier);
		return tempCatalogFunctions.containsKey(normalizedIdentifier);
	}

	/**
	 * Check whether a temporary system function is already registered.
	 * @param functionName the name of the function
	 * @return whether the temporary system function exists in the function catalog
	 */
	public boolean hasTemporarySystemFunction(String functionName) {
		return tempSystemFunctions.containsKey(functionName);
	}

	/**
	 * Drop a temporary system function.
	 *
	 * @param funcName name of the function
	 * @param ignoreIfNotExist Flag to specify behavior when the function does not exist:
	 *                         if set to false, throw an exception,
	 *                         if set to true, do nothing.
	 */
	public void dropTempSystemFunction(String funcName, boolean ignoreIfNotExist) {
		String normalizedName = FunctionIdentifier.normalizeName(funcName);

		FunctionDefinition fd = tempSystemFunctions.remove(normalizedName);

		if (fd == null && !ignoreIfNotExist) {
			throw new ValidationException(String.format("Temporary system function %s doesn't exist", funcName));
		}
	}

	/**
	 * Drop a temporary catalog function.
	 *
	 * @param identifier identifier of the function
	 * @param ignoreIfNotExist Flag to specify behavior when the function does not exist:
	 *                         if set to false, throw an exception,
	 *                         if set to true, do nothing.
	 */
	public void dropTempCatalogFunction(ObjectIdentifier identifier, boolean ignoreIfNotExist) {
		ObjectIdentifier normalizedName = FunctionIdentifier.normalizeObjectIdentifier(identifier);

		FunctionDefinition fd = tempCatalogFunctions.remove(normalizedName);

		if (fd == null && !ignoreIfNotExist) {
			throw new ValidationException(String.format("Temporary catalog function %s doesn't exist", identifier));
		}
	}

	/**
	 * Get names of all user defined functions, including temp system functions, temp catalog functions and catalog functions
	 * in the current catalog and current database.
	 */
	public String[] getUserDefinedFunctions() {
		return getUserDefinedFunctionNames().toArray(new String[0]);
	}

	/**
	 * Get names of all functions, including temp system functions, system functions, temp catalog functions and catalog functions
	 * in the current catalog and current database.
	 */
	public String[] getFunctions() {
		Set<String> result = getUserDefinedFunctionNames();

		// add system functions
		result.addAll(moduleManager.listFunctions());

		return result.toArray(new String[0]);
	}

	private Set<String> getUserDefinedFunctionNames() {

		// add temp system functions
		Set<String> result = new HashSet<>(tempSystemFunctions.keySet());

		String currentCatalog = catalogManager.getCurrentCatalog();
		String currentDatabase = catalogManager.getCurrentDatabase();

		// add temp catalog functions
		result.addAll(tempCatalogFunctions.keySet().stream()
			.filter(oi -> oi.getCatalogName().equals(currentCatalog)
				&& oi.getDatabaseName().equals(currentDatabase))
			.map(ObjectIdentifier::getObjectName)
			.collect(Collectors.toSet())
		);

		// add catalog functions
		Catalog catalog = catalogManager.getCatalog(currentCatalog).get();
		try {
			result.addAll(catalog.listFunctions(currentDatabase));
		} catch (DatabaseNotExistException e) {
			// Ignore since there will always be a current database of the current catalog
		}

		return result;
	}

	@Override
	public Optional<FunctionLookup.Result> lookupFunction(UnresolvedIdentifier identifier) {
		// precise function reference
		if (identifier.getDatabaseName().isPresent()) {
			return resolvePreciseFunctionReference(catalogManager.qualifyIdentifier(identifier));
		} else {
			// ambiguous function reference
			return resolveAmbiguousFunctionReference(identifier.getObjectName());
		}
	}

	private Optional<FunctionLookup.Result> resolvePreciseFunctionReference(ObjectIdentifier oi) {
		// resolve order:
		// 1. Temporary functions
		// 2. Catalog functions
		ObjectIdentifier normalizedIdentifier = FunctionIdentifier.normalizeObjectIdentifier(oi);
		FunctionDefinition potentialResult = tempCatalogFunctions.get(normalizedIdentifier);

		if (potentialResult != null) {
			return Optional.of(
				new FunctionLookup.Result(
					FunctionIdentifier.of(oi),
					potentialResult
				)
			);
		}

		Optional<Catalog> catalogOptional = catalogManager.getCatalog(oi.getCatalogName());

		if (catalogOptional.isPresent()) {
			Catalog catalog = catalogOptional.get();
			try {
				CatalogFunction catalogFunction = catalog.getFunction(
					new ObjectPath(oi.getDatabaseName(), oi.getObjectName()));

				FunctionDefinition fd;
				if (catalog.getFunctionDefinitionFactory().isPresent()) {
					fd = catalog.getFunctionDefinitionFactory().get()
						.createFunctionDefinition(oi.getObjectName(), catalogFunction);
				} else {
					fd = FunctionDefinitionUtil.createFunctionDefinition(
						oi.getObjectName(), catalogFunction.getClassName());
				}

				return Optional.of(
					new FunctionLookup.Result(FunctionIdentifier.of(oi), fd));
			} catch (FunctionNotExistException e) {
				// Ignore
			}
		}

		return Optional.empty();
	}

	private Optional<FunctionLookup.Result> resolveAmbiguousFunctionReference(String funcName) {
		// resolve order:
		// 1. Temporary system functions
		// 2. System functions
		// 3. Temporary catalog functions
		// 4. Catalog functions

		String normalizedName = FunctionIdentifier.normalizeName(funcName);
		if (tempSystemFunctions.containsKey(normalizedName)) {
			return Optional.of(
				new FunctionLookup.Result(
					FunctionIdentifier.of(funcName),
					tempSystemFunctions.get(normalizedName))
			);
		}

		Optional<FunctionDefinition> candidate = moduleManager.getFunctionDefinition(normalizedName);
		ObjectIdentifier oi = ObjectIdentifier.of(
			catalogManager.getCurrentCatalog(),
			catalogManager.getCurrentDatabase(),
			funcName);

		return candidate.map(fd ->
			Optional.of(new Result(FunctionIdentifier.of(funcName), fd)
		)).orElseGet(() -> resolvePreciseFunctionReference(oi));
	}

	@Override
	public PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
		Preconditions.checkNotNull(
			plannerTypeInferenceUtil,
			"A planner should have set the type inference utility.");
		return plannerTypeInferenceUtil;
	}

	private void registerTempSystemFunction(String name, FunctionDefinition functionDefinition) {
		tempSystemFunctions.put(FunctionIdentifier.normalizeName(name), functionDefinition);
	}

	private void registerTempCatalogFunction(ObjectIdentifier oi, FunctionDefinition functionDefinition) {
		tempCatalogFunctions.put(FunctionIdentifier.normalizeObjectIdentifier(oi), functionDefinition);
	}
}

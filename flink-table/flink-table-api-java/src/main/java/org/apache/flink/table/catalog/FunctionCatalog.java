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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ImperativeAggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple function catalog to store {@link FunctionDefinition}s in catalogs.
 *
 * <p>Note: This class can be cleaned up a lot once we drop the methods deprecated as part of
 * FLIP-65. In the long-term, the class should be a part of catalog manager similar to {@link
 * DataTypeFactory}.
 */
@Internal
public final class FunctionCatalog {
    private final ReadableConfig config;
    private final ResourceManager resourceManager;
    private final CatalogManager catalogManager;
    private final ModuleManager moduleManager;

    private final Map<String, CatalogFunction> tempSystemFunctions = new LinkedHashMap<>();
    private final Map<ObjectIdentifier, CatalogFunction> tempCatalogFunctions =
            new LinkedHashMap<>();

    /**
     * Temporary utility until the new type inference is fully functional. It needs to be set by the
     * planner.
     */
    private PlannerTypeInferenceUtil plannerTypeInferenceUtil;

    public FunctionCatalog(
            ReadableConfig config,
            ResourceManager resourceManager,
            CatalogManager catalogManager,
            ModuleManager moduleManager) {
        this.config = checkNotNull(config);
        this.resourceManager = checkNotNull(resourceManager);
        this.catalogManager = checkNotNull(catalogManager);
        this.moduleManager = checkNotNull(moduleManager);
    }

    public void setPlannerTypeInferenceUtil(PlannerTypeInferenceUtil plannerTypeInferenceUtil) {
        this.plannerTypeInferenceUtil = plannerTypeInferenceUtil;
    }

    /** Registers a temporary system function. */
    public void registerTemporarySystemFunction(
            String name, FunctionDefinition definition, boolean ignoreIfExists) {
        registerTemporarySystemFunction(
                name, new InlineCatalogFunction(definition), ignoreIfExists);
    }

    /** Registers a uninstantiated temporary system function. */
    public void registerTemporarySystemFunction(
            String name,
            String fullyQualifiedName,
            FunctionLanguage language,
            boolean ignoreIfExists) {
        registerTemporarySystemFunction(
                name, new CatalogFunctionImpl(fullyQualifiedName, language), ignoreIfExists);
    }

    /** Registers a temporary system function from resource uris. */
    public void registerTemporarySystemFunction(
            String name, String className, List<ResourceUri> resourceUris) {
        registerTemporarySystemFunction(
                name,
                new CatalogFunctionImpl(className, FunctionLanguage.JAVA, resourceUris),
                false);
    }

    /** Drops a temporary system function. Returns true if a function was dropped. */
    public boolean dropTemporarySystemFunction(String name, boolean ignoreIfNotExist) {
        final String normalizedName = FunctionIdentifier.normalizeName(name);
        final CatalogFunction function = tempSystemFunctions.remove(normalizedName);

        if (function == null && !ignoreIfNotExist) {
            throw new ValidationException(
                    String.format(
                            "Could not drop temporary system function. A function named '%s' doesn't exist.",
                            name));
        }

        return function != null;
    }

    /** Registers a temporary catalog function. */
    public void registerTemporaryCatalogFunction(
            UnresolvedIdentifier unresolvedIdentifier,
            FunctionDefinition definition,
            boolean ignoreIfExists) {
        registerTemporaryCatalogFunction(
                unresolvedIdentifier, new InlineCatalogFunction(definition), ignoreIfExists);
    }

    /** Registers a uninstantiated temporary catalog function. */
    public void registerTemporaryCatalogFunction(
            UnresolvedIdentifier unresolvedIdentifier,
            CatalogFunction catalogFunction,
            boolean ignoreIfExists) {
        final ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        final ObjectIdentifier normalizedIdentifier =
                FunctionIdentifier.normalizeObjectIdentifier(identifier);

        if (!tempCatalogFunctions.containsKey(normalizedIdentifier)) {
            Optional<TemporaryOperationListener> listener =
                    catalogManager.getTemporaryOperationListener(normalizedIdentifier);
            if (listener.isPresent()) {
                catalogFunction =
                        listener.get()
                                .onCreateTemporaryFunction(
                                        normalizedIdentifier.toObjectPath(), catalogFunction);
            }
            try {
                validateAndPrepareFunction(identifier.asSummaryString(), catalogFunction);
            } catch (Throwable t) {
                throw new ValidationException(
                        String.format(
                                "Could not register temporary catalog function '%s' due to implementation errors.",
                                identifier.asSummaryString()),
                        t);
            }
            tempCatalogFunctions.put(normalizedIdentifier, catalogFunction);
        } else if (!ignoreIfExists) {
            throw new ValidationException(
                    String.format(
                            "Could not register temporary catalog function. A function '%s' does already exist.",
                            identifier.asSummaryString()));
        }
    }

    /** Drops a temporary catalog function. Returns true if a function was dropped. */
    public boolean dropTemporaryCatalogFunction(
            UnresolvedIdentifier unresolvedIdentifier, boolean ignoreIfNotExist) {
        final ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        return dropTempCatalogFunction(identifier, ignoreIfNotExist) != null;
    }

    /** Registers a catalog function by also considering temporary catalog functions. */
    public void registerCatalogFunction(
            UnresolvedIdentifier unresolvedIdentifier,
            Class<? extends UserDefinedFunction> functionClass,
            boolean ignoreIfExists) {
        final ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        final CatalogFunction catalogFunction =
                new CatalogFunctionImpl(functionClass.getName(), FunctionLanguage.JAVA);

        try {
            UserDefinedFunctionHelper.validateClass(functionClass);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "Could not register catalog function '%s' due to implementation errors.",
                            identifier.asSummaryString()),
                    t);
        }

        registerCatalogFunction(identifier, catalogFunction, ignoreIfExists);
    }

    public void registerCatalogFunction(
            UnresolvedIdentifier unresolvedIdentifier,
            String className,
            List<ResourceUri> resourceUris,
            boolean ignoreIfExists) {

        final ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        final CatalogFunction catalogFunction =
                new CatalogFunctionImpl(className, FunctionLanguage.JAVA, resourceUris);

        registerCatalogFunction(identifier, catalogFunction, ignoreIfExists);
    }

    /**
     * Drops a catalog function by also considering temporary catalog functions. Returns true if a
     * function was dropped.
     */
    public boolean dropCatalogFunction(
            UnresolvedIdentifier unresolvedIdentifier, boolean ignoreIfNotExist) {
        final ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        final ObjectIdentifier normalizedIdentifier =
                FunctionIdentifier.normalizeObjectIdentifier(identifier);

        final Catalog catalog =
                catalogManager
                        .getCatalog(normalizedIdentifier.getCatalogName())
                        .orElseThrow(IllegalStateException::new);
        final ObjectPath path = identifier.toObjectPath();

        // we force users to deal with temporary catalog functions first
        if (tempCatalogFunctions.containsKey(normalizedIdentifier)) {
            throw new ValidationException(
                    String.format(
                            "Could not drop catalog function. A temporary function '%s' does already exist. "
                                    + "Please drop the temporary function first.",
                            identifier.asSummaryString()));
        }

        if (!catalog.functionExists(path)) {
            if (ignoreIfNotExist) {
                return false;
            }
            throw new ValidationException(
                    String.format(
                            "Could not drop catalog function. A function '%s' doesn't exist.",
                            identifier.asSummaryString()));
        }

        try {
            catalog.dropFunction(path, ignoreIfNotExist);
        } catch (Throwable t) {
            throw new TableException(
                    String.format(
                            "Could not drop catalog function '%s'.", identifier.asSummaryString()),
                    t);
        }
        return true;
    }

    /**
     * Get names of all user defined functions, including temp system functions, temp catalog
     * functions and catalog functions in the current catalog and current database.
     */
    public String[] getUserDefinedFunctions() {
        return getUserDefinedFunctions(
                        catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase())
                .stream()
                .map(FunctionIdentifier::getFunctionName)
                .toArray(String[]::new);
    }

    /**
     * Get names of all user including temp system functions, temp catalog * functions and catalog
     * functions in the specified catalog and specified database.
     */
    public Set<FunctionIdentifier> getUserDefinedFunctions(
            String catalogName, String databaseName) {
        // add temp system functions
        Set<FunctionIdentifier> result =
                tempSystemFunctions.keySet().stream()
                        .map(FunctionIdentifier::of)
                        .collect(Collectors.toSet());

        // add temp catalog functions
        result.addAll(
                tempCatalogFunctions.keySet().stream()
                        .filter(
                                oi ->
                                        oi.getCatalogName().equals(catalogName)
                                                && oi.getDatabaseName().equals(databaseName))
                        .map(FunctionIdentifier::of)
                        .collect(Collectors.toSet()));

        // add catalog functions if catalog exists
        catalogManager
                .getCatalog(catalogName)
                .ifPresent(
                        catalog ->
                                result.addAll(
                                        getCatalogFunctions(catalog, catalogName, databaseName)));

        return result;
    }

    private static Collection<FunctionIdentifier> getCatalogFunctions(
            Catalog catalog, String catalogName, String databaseName) {
        try {
            return catalog.listFunctions(databaseName).stream()
                    .map(
                            name -> {
                                final ObjectIdentifier identifier =
                                        ObjectIdentifier.of(catalogName, databaseName, name);
                                return FunctionIdentifier.of(identifier);
                            })
                    .collect(Collectors.toList());
        } catch (DatabaseNotExistException e) {
            // if database does not exist, do not add catalog functions
            return Collections.emptyList();
        }
    }

    /**
     * Get names of all functions, including temp system functions, system functions, temp catalog
     * functions and catalog functions in the current catalog and current database.
     */
    public String[] getFunctions() {
        return getFunctions(
                catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase());
    }

    /**
     * Get names of all functions, including temp system functions, system functions, temp catalog
     * functions and catalog functions with specific catalog and database.
     */
    public String[] getFunctions(String catalogName, String databaseName) {
        Set<String> result =
                getUserDefinedFunctions(catalogName, databaseName).stream()
                        .map(FunctionIdentifier::getFunctionName)
                        .collect(Collectors.toSet());

        // add system functions
        result.addAll(moduleManager.listFunctions());

        return result.toArray(new String[0]);
    }

    /**
     * Check whether a temporary catalog function is already registered.
     *
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
     *
     * @param functionName the name of the function
     * @return whether the temporary system function exists in the function catalog
     */
    public boolean hasTemporarySystemFunction(String functionName) {
        return tempSystemFunctions.containsKey(functionName);
    }

    /**
     * Creates a {@link FunctionLookup} to this {@link FunctionCatalog}.
     *
     * @param parser parser to use for parsing identifiers
     */
    public FunctionLookup asLookup(Function<String, UnresolvedIdentifier> parser) {
        return new FunctionLookup() {
            @Override
            public Optional<ContextResolvedFunction> lookupFunction(String stringIdentifier) {
                UnresolvedIdentifier unresolvedIdentifier = parser.apply(stringIdentifier);
                return lookupFunction(unresolvedIdentifier);
            }

            @Override
            public Optional<ContextResolvedFunction> lookupFunction(
                    UnresolvedIdentifier identifier) {
                return FunctionCatalog.this.lookupFunction(identifier);
            }

            @Override
            public PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
                Preconditions.checkNotNull(
                        plannerTypeInferenceUtil,
                        "A planner should have set the type inference utility.");
                return plannerTypeInferenceUtil;
            }
        };
    }

    public Optional<ContextResolvedProcedure> lookupProcedure(UnresolvedIdentifier identifier) {
        ObjectIdentifier procedureIdentifier = catalogManager.qualifyIdentifier(identifier);
        Optional<Catalog> catalog = catalogManager.getCatalog(procedureIdentifier.getCatalogName());
        if (catalog.isPresent()) {
            Procedure procedure;
            try {
                procedure = catalog.get().getProcedure(procedureIdentifier.toObjectPath());
            } catch (ProcedureNotExistException e) {
                return Optional.empty();
            }
            return Optional.of(
                    new ContextResolvedProcedure(
                            FunctionIdentifier.of(procedureIdentifier), procedure));
        }
        return Optional.empty();
    }

    public Optional<ContextResolvedFunction> lookupFunction(UnresolvedIdentifier identifier) {
        // precise function reference
        if (identifier.getDatabaseName().isPresent()) {
            return resolvePreciseFunctionReference(catalogManager.qualifyIdentifier(identifier));
        } else {
            // ambiguous function reference
            return resolveAmbiguousFunctionReference(identifier.getObjectName());
        }
    }

    // --------------------------------------------------------------------------------------------
    // Legacy function handling before FLIP-65
    // --------------------------------------------------------------------------------------------

    /**
     * @deprecated Use {@link #registerTemporarySystemFunction(String, FunctionDefinition, boolean)}
     *     instead.
     */
    @Deprecated
    public void registerTempSystemScalarFunction(String name, ScalarFunction function) {
        UserDefinedFunctionHelper.prepareInstance(config, function);

        registerTempSystemFunction(name, new ScalarFunctionDefinition(name, function));
    }

    /**
     * @deprecated Use {@link #registerTemporarySystemFunction(String, FunctionDefinition, boolean)}
     *     instead.
     */
    @Deprecated
    public <T> void registerTempSystemTableFunction(
            String name, TableFunction<T> function, TypeInformation<T> resultType) {
        UserDefinedFunctionHelper.prepareInstance(config, function);

        registerTempSystemFunction(name, new TableFunctionDefinition(name, function, resultType));
    }

    /**
     * @deprecated Use {@link #registerTemporarySystemFunction(String, FunctionDefinition, boolean)}
     *     instead.
     */
    @Deprecated
    public <T, ACC> void registerTempSystemAggregateFunction(
            String name,
            ImperativeAggregateFunction<T, ACC> function,
            TypeInformation<T> resultType,
            TypeInformation<ACC> accType) {
        UserDefinedFunctionHelper.prepareInstance(config, function);

        final FunctionDefinition definition;
        if (function instanceof AggregateFunction) {
            definition =
                    new AggregateFunctionDefinition(
                            name, (AggregateFunction<?, ?>) function, resultType, accType);
        } else if (function instanceof TableAggregateFunction) {
            definition =
                    new TableAggregateFunctionDefinition(
                            name, (TableAggregateFunction<?, ?>) function, resultType, accType);
        } else {
            throw new TableException("Unknown function class: " + function.getClass());
        }

        registerTempSystemFunction(name, definition);
    }

    /**
     * @deprecated Use {@link #registerTemporaryCatalogFunction(UnresolvedIdentifier,
     *     FunctionDefinition, boolean)} instead.
     */
    @Deprecated
    public void registerTempCatalogScalarFunction(ObjectIdentifier oi, ScalarFunction function) {
        UserDefinedFunctionHelper.prepareInstance(config, function);

        registerTempCatalogFunction(oi, new ScalarFunctionDefinition(oi.getObjectName(), function));
    }

    /**
     * Drop a temporary catalog function.
     *
     * @param identifier identifier of the function
     * @param ignoreIfNotExist Flag to specify behavior when the function does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @return the removed catalog function, which is null if function doesn't exist and
     *     ignoreIfNotExist is true.
     */
    public CatalogFunction dropTempCatalogFunction(
            ObjectIdentifier identifier, boolean ignoreIfNotExist) {
        ObjectIdentifier normalizedName = FunctionIdentifier.normalizeObjectIdentifier(identifier);

        CatalogFunction fd = tempCatalogFunctions.get(normalizedName);

        if (fd != null) {
            catalogManager
                    .getTemporaryOperationListener(normalizedName)
                    .ifPresent(l -> l.onDropTemporaryFunction(normalizedName.toObjectPath()));
            tempCatalogFunctions.remove(normalizedName);
        } else if (!ignoreIfNotExist) {
            throw new ValidationException(
                    String.format("Temporary catalog function %s doesn't exist", identifier));
        }
        return fd;
    }

    /**
     * @deprecated Use {@link #registerTemporarySystemFunction(String, FunctionDefinition, boolean)}
     *     instead.
     */
    @Deprecated
    private void registerTempSystemFunction(String name, FunctionDefinition functionDefinition) {
        // This method is called by the interface which uses the old type inference,
        // e.g. TableEnvironment#registerFunction
        // In this case the UDF is wrapped by ScalarFunctionDefinition, TableFunctionDefinition,
        // etc.
        // The raw UDFs will be validated and cleaned before being wrapped, so just put them to the
        // map
        // in this method.
        tempSystemFunctions.put(
                FunctionIdentifier.normalizeName(name),
                new InlineCatalogFunction(functionDefinition));
    }

    /**
     * @deprecated Use {@link #registerTemporaryCatalogFunction(UnresolvedIdentifier,
     *     FunctionDefinition, boolean)} instead.
     */
    @Deprecated
    private void registerTempCatalogFunction(
            ObjectIdentifier oi, FunctionDefinition functionDefinition) {
        // This method is called by the interface which uses the old type inference,
        // but there is no TableEnvironment-level public API uses this method now.
        // In this case the UDFs are wrapped by ScalarFunctionDefinition, TableFunctionDefinition,
        // etc.
        // The raw UDFs will be validated and cleaned before being wrapped, so just put them to the
        // map
        // in this method.
        tempCatalogFunctions.put(
                FunctionIdentifier.normalizeObjectIdentifier(oi),
                new InlineCatalogFunction(functionDefinition));
    }

    public void registerTemporarySystemFunction(
            String name, CatalogFunction function, boolean ignoreIfExists) {
        final String normalizedName = FunctionIdentifier.normalizeName(name);

        try {
            validateAndPrepareFunction(name, function);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "Could not register temporary system function '%s' due to implementation errors.",
                            name),
                    t);
        }

        if (!tempSystemFunctions.containsKey(normalizedName)) {
            tempSystemFunctions.put(normalizedName, function);
        } else if (!ignoreIfExists) {
            throw new ValidationException(
                    String.format(
                            "Could not register temporary system function. A function named '%s' does already exist.",
                            name));
        }
    }

    // --------------------------------------------------------------------------------------------
    private Optional<ContextResolvedFunction> resolvePreciseFunctionReference(String funcName) {
        if (StringUtils.isNullOrWhitespaceOnly(catalogManager.getCurrentCatalog())
                || StringUtils.isNullOrWhitespaceOnly(catalogManager.getCurrentDatabase())) {
            return Optional.empty();
        } else {
            ObjectIdentifier oi =
                    ObjectIdentifier.of(
                            catalogManager.getCurrentCatalog(),
                            catalogManager.getCurrentDatabase(),
                            funcName);
            return resolvePreciseFunctionReference(oi);
        }
    }

    private Optional<ContextResolvedFunction> resolvePreciseFunctionReference(ObjectIdentifier oi) {
        // resolve order:
        // 1. Temporary functions
        // 2. Catalog functions
        ObjectIdentifier normalizedIdentifier = FunctionIdentifier.normalizeObjectIdentifier(oi);
        CatalogFunction potentialResult = tempCatalogFunctions.get(normalizedIdentifier);

        if (potentialResult != null) {
            return Optional.of(
                    ContextResolvedFunction.temporary(
                            FunctionIdentifier.of(oi),
                            getFunctionDefinition(oi.getObjectName(), potentialResult)));
        }

        Optional<Catalog> catalogOptional = catalogManager.getCatalog(oi.getCatalogName());

        if (catalogOptional.isPresent()) {
            Catalog catalog = catalogOptional.get();
            try {
                CatalogFunction catalogFunction =
                        catalog.getFunction(
                                new ObjectPath(oi.getDatabaseName(), oi.getObjectName()));

                FunctionDefinition fd;
                if (catalog.getFunctionDefinitionFactory().isPresent()
                        && catalogFunction.getFunctionLanguage() != FunctionLanguage.PYTHON) {
                    registerFunctionJarResources(
                            oi.asSummaryString(), catalogFunction.getFunctionResources());
                    fd =
                            catalog.getFunctionDefinitionFactory()
                                    .get()
                                    .createFunctionDefinition(
                                            oi.getObjectName(),
                                            catalogFunction,
                                            resourceManager::getUserClassLoader);
                } else {
                    fd = getFunctionDefinition(oi.asSummaryString(), catalogFunction);
                }

                return Optional.of(
                        ContextResolvedFunction.permanent(FunctionIdentifier.of(oi), fd));
            } catch (FunctionNotExistException e) {
                // Ignore
            }
        }

        return Optional.empty();
    }

    private Optional<ContextResolvedFunction> resolveAmbiguousFunctionReference(String funcName) {
        // resolve order:
        // 1. Temporary system functions
        // 2. System functions
        // 3. Temporary catalog functions
        // 4. Catalog functions

        String normalizedName = FunctionIdentifier.normalizeName(funcName);
        if (tempSystemFunctions.containsKey(normalizedName)) {
            return Optional.of(
                    ContextResolvedFunction.temporary(
                            FunctionIdentifier.of(funcName),
                            getFunctionDefinition(
                                    normalizedName, tempSystemFunctions.get(normalizedName))));
        }

        Optional<FunctionDefinition> candidate =
                moduleManager.getFunctionDefinition(normalizedName);

        return candidate
                .map(
                        fd ->
                                Optional.of(
                                        ContextResolvedFunction.permanent(
                                                FunctionIdentifier.of(funcName), fd)))
                .orElseGet(() -> resolvePreciseFunctionReference(funcName));
    }

    @SuppressWarnings("unchecked")
    private void validateAndPrepareFunction(String name, CatalogFunction function)
            throws ClassNotFoundException {
        // If the input is instance of UserDefinedFunction, it means it uses the new type inference.
        // In this situation the UDF have not been validated and cleaned, so we need to validate it
        // and clean its closure here.
        // If the input is instance of `ScalarFunctionDefinition`, `TableFunctionDefinition` and so
        // on, it means it uses the old type inference. We assume that they have been validated
        // before being wrapped.
        if (function instanceof InlineCatalogFunction) {
            FunctionDefinition definition = ((InlineCatalogFunction) function).getDefinition();
            if (definition instanceof UserDefinedFunction) {
                UserDefinedFunctionHelper.prepareInstance(config, (UserDefinedFunction) definition);
            }
            // Skip validation if it's not a UserDefinedFunction.
        } else if (function.getFunctionLanguage() == FunctionLanguage.JAVA) {
            // If the jar resource of UDF used is not empty, register it to classloader before
            // validate.
            registerFunctionJarResources(name, function.getFunctionResources());

            UserDefinedFunctionHelper.validateClass(
                    (Class<? extends UserDefinedFunction>)
                            resourceManager
                                    .getUserClassLoader()
                                    .loadClass(function.getClassName()));
        }
    }

    private FunctionDefinition getFunctionDefinition(String name, CatalogFunction function) {
        if (function instanceof InlineCatalogFunction) {
            // The instantiated UDFs have been validated and cleaned when registering, just return
            // them
            // directly.
            return ((InlineCatalogFunction) function).getDefinition();
        }
        // If the jar resource of UDF used is not empty, register it to classloader before
        // validate.
        registerFunctionJarResources(name, function.getFunctionResources());

        return UserDefinedFunctionHelper.instantiateFunction(
                resourceManager.getUserClassLoader(),
                // future
                config,
                name,
                function);
    }

    public void registerFunctionJarResources(String functionName, List<ResourceUri> resourceUris) {
        try {
            if (!resourceUris.isEmpty()) {
                resourceManager.registerJarResources(resourceUris);
            }
        } catch (Exception e) {
            throw new TableException(
                    String.format(
                            "Failed to register jar resource '%s' of function '%s'.",
                            resourceUris, functionName),
                    e);
        }
    }

    private void registerCatalogFunction(
            ObjectIdentifier identifier, CatalogFunction catalogFunction, boolean ignoreIfExists) {
        final ObjectIdentifier normalizedIdentifier =
                FunctionIdentifier.normalizeObjectIdentifier(identifier);

        final Catalog catalog =
                catalogManager
                        .getCatalog(normalizedIdentifier.getCatalogName())
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Catalog %s not exists.",
                                                        normalizedIdentifier.getCatalogName())));

        final ObjectPath path = identifier.toObjectPath();

        // we force users to deal with temporary catalog functions first
        if (tempCatalogFunctions.containsKey(normalizedIdentifier)) {
            if (ignoreIfExists) {
                return;
            }
            throw new ValidationException(
                    String.format(
                            "Could not register catalog function. A temporary function '%s' does already exist. "
                                    + "Please drop the temporary function first.",
                            identifier.asSummaryString()));
        }

        if (catalog.functionExists(path)) {
            if (ignoreIfExists) {
                return;
            }
            throw new ValidationException(
                    String.format(
                            "Could not register catalog function. A function '%s' does already exist.",
                            identifier.asSummaryString()));
        }

        try {
            catalog.createFunction(path, catalogFunction, ignoreIfExists);
        } catch (Throwable t) {
            throw new TableException(
                    String.format(
                            "Could not register catalog function '%s'.",
                            identifier.asSummaryString()),
                    t);
        }
    }

    /** The CatalogFunction which holds an instantiated UDF. */
    @Internal
    public static class InlineCatalogFunction implements CatalogFunction {

        private final FunctionDefinition definition;

        public InlineCatalogFunction(FunctionDefinition definition) {
            this.definition = definition;
        }

        @Override
        public String getClassName() {
            // Not all instantiated UDFs have a class name, such as Python Lambda UDF. Even if the
            // UDF
            // has a class name, there is no guarantee that the new UDF object constructed from the
            // class name is the same as the UDF held by this object. To reduce the chance of making
            // mistakes, UnsupportedOperationException is thrown here.
            throw new UnsupportedOperationException(
                    "This CatalogFunction is a InlineCatalogFunction. This method should not be called.");
        }

        @Override
        public CatalogFunction copy() {
            return new InlineCatalogFunction(definition);
        }

        @Override
        public Optional<String> getDescription() {
            return Optional.empty();
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return Optional.empty();
        }

        @Override
        public boolean isGeneric() {
            throw new UnsupportedOperationException(
                    "This CatalogFunction is a InlineCatalogFunction. This method should not be called.");
        }

        @Override
        public FunctionLanguage getFunctionLanguage() {
            return FunctionLanguage.JAVA;
        }

        @Override
        public List<ResourceUri> getFunctionResources() {
            return Collections.emptyList();
        }

        public FunctionDefinition getDefinition() {
            return definition;
        }
    }
}

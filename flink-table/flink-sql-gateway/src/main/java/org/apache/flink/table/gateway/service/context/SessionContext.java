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

package org.apache.flink.table.gateway.service.context;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Context describing a session, it's mainly used for user to open a new session in the backend. If
 * client request to open a new session, the backend {@code Executor} will maintain the session
 * context map util users close it.
 */
public class SessionContext {

    private static final Logger LOG = LoggerFactory.getLogger(SessionContext.class);

    private final DefaultContext defaultContext;
    private final SessionHandle sessionId;
    private final EndpointVersion endpointVersion;

    // store all options and use Configuration to build SessionState and TableConfig.
    private final Configuration sessionConf;
    private final SessionState sessionState;
    private final URLClassLoader userClassloader;

    private final OperationManager operationManager;

    private SessionContext(
            DefaultContext defaultContext,
            SessionHandle sessionId,
            EndpointVersion endpointVersion,
            Configuration sessionConf,
            URLClassLoader classLoader,
            SessionState sessionState,
            OperationManager operationManager) {
        this.defaultContext = defaultContext;
        this.sessionId = sessionId;
        this.endpointVersion = endpointVersion;
        this.sessionConf = sessionConf;
        this.userClassloader = classLoader;
        this.sessionState = sessionState;
        this.operationManager = operationManager;
    }

    // --------------------------------------------------------------------------------------------
    // Getter/Setter
    // --------------------------------------------------------------------------------------------

    public SessionHandle getSessionId() {
        return this.sessionId;
    }

    public Map<String, String> getConfigMap() {
        return sessionConf.toMap();
    }

    public OperationManager getOperationManager() {
        return operationManager;
    }

    public EndpointVersion getEndpointVersion() {
        return endpointVersion;
    }

    public SessionState getSessionState() {
        return sessionState;
    }

    public void set(String key, String value) {
        try {
            // Test whether the key value will influence the creation of the Executor.
            createOperationExecutor(Configuration.fromMap(Collections.singletonMap(key, value)));
        } catch (Exception e) {
            // get error and reset the key with old value
            throw new SqlExecutionException(
                    String.format("Failed to set key %s with value %s.", key, value), e);
        }
        sessionConf.setString(key, value);
    }

    public synchronized void reset(String key) {
        Configuration configuration = defaultContext.getFlinkConfig();
        // If the key exist in default yaml, reset to default
        ConfigOption<String> option = ConfigOptions.key(key).stringType().noDefaultValue();
        if (configuration.contains(option)) {
            String defaultValue = configuration.get(option);
            set(key, defaultValue);
        } else {
            sessionConf.removeConfig(option);
        }
    }

    public synchronized void reset() {
        for (String key : sessionConf.keySet()) {
            sessionConf.removeConfig(ConfigOptions.key(key).stringType().noDefaultValue());
        }
        sessionConf.addAll(defaultContext.getFlinkConfig());
    }

    // --------------------------------------------------------------------------------------------
    // Method to execute commands
    // --------------------------------------------------------------------------------------------

    public OperationExecutor createOperationExecutor(Configuration executionConfig) {
        return new OperationExecutor(this, executionConfig);
    }

    /** Close resources, e.g. catalogs. */
    public void close() {
        operationManager.close();
        for (String name : sessionState.catalogManager.listCatalogs()) {
            try {
                sessionState.catalogManager.getCatalog(name).ifPresent(Catalog::close);
            } catch (Throwable t) {
                LOG.error(
                        String.format(
                                "Failed to close catalog %s for the session %s.", name, sessionId),
                        t);
            }
        }
        try {
            userClassloader.close();
        } catch (IOException e) {
            LOG.error(
                    String.format(
                            "Error while closing class loader for the session %s.", sessionId),
                    e);
        }
        try {
            sessionState.resourceManager.close();
        } catch (IOException e) {
            LOG.error(
                    String.format(
                            "Failed to close the resource manager for the session %s.", sessionId),
                    e);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static SessionContext create(
            DefaultContext defaultContext,
            SessionHandle sessionId,
            SessionEnvironment environment,
            ExecutorService operationExecutorService) {
        // --------------------------------------------------------------------------------------------------------------
        // Init config
        // --------------------------------------------------------------------------------------------------------------

        Configuration configuration = defaultContext.getFlinkConfig().clone();
        configuration.addAll(Configuration.fromMap(environment.getSessionConfig()));
        // every session configure the specific local resource download directory
        setResourceDownloadTmpDir(configuration, sessionId);

        // --------------------------------------------------------------------------------------------------------------
        // Init classloader
        // --------------------------------------------------------------------------------------------------------------

        final MutableURLClassLoader userClassLoader =
                FlinkUserCodeClassLoaders.create(
                        new URL[0], SessionContext.class.getClassLoader(), configuration);

        // --------------------------------------------------------------------------------------------------------------
        // Init session state
        // --------------------------------------------------------------------------------------------------------------

        final ResourceManager resourceManager = new ResourceManager(configuration, userClassLoader);

        final ModuleManager moduleManager = buildModuleManager(environment);

        final CatalogManager catalogManager =
                buildCatalogManager(configuration, userClassLoader, environment);

        final FunctionCatalog functionCatalog =
                new FunctionCatalog(configuration, resourceManager, catalogManager, moduleManager);
        SessionState sessionState =
                new SessionState(catalogManager, moduleManager, resourceManager, functionCatalog);

        return new SessionContext(
                defaultContext,
                sessionId,
                environment.getSessionEndpointVersion(),
                configuration,
                userClassLoader,
                sessionState,
                new OperationManager(operationExecutorService));
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------------------------

    public TableEnvironmentInternal createTableEnvironment() {
        // checks the value of RUNTIME_MODE
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(sessionConf).build();

        StreamExecutionEnvironment streamExecEnv = createStreamExecutionEnvironment();

        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(defaultContext.getFlinkConfig());
        tableConfig.addConfiguration(sessionConf);

        final Executor executor = lookupExecutor(streamExecEnv, userClassloader);
        return createStreamTableEnvironment(
                streamExecEnv,
                settings,
                tableConfig,
                executor,
                sessionState.catalogManager,
                sessionState.moduleManager,
                sessionState.resourceManager,
                sessionState.functionCatalog);
    }

    private TableEnvironmentInternal createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            TableConfig tableConfig,
            Executor executor,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            ResourceManager resourceManager,
            FunctionCatalog functionCatalog) {

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        resourceManager.getUserClassLoader(),
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        try {
            return new StreamTableEnvironmentImpl(
                    catalogManager,
                    moduleManager,
                    resourceManager,
                    functionCatalog,
                    tableConfig,
                    env,
                    planner,
                    executor,
                    settings.isStreamingMode());
        } catch (ValidationException e) {
            if (tableConfig.getSqlDialect() == SqlDialect.HIVE) {
                String additionErrorMsg =
                        "Note: if you want to use Hive dialect, "
                                + "please first move the jar `flink-table-planner_2.12` located in `FLINK_HOME/opt` "
                                + "to `FLINK_HOME/lib` and then move out the jar `flink-table-planner-loader` from `FLINK_HOME/lib`.";
                ExceptionUtils.updateDetailMessage(e, t -> t.getMessage() + additionErrorMsg);
            }
            throw e;
        }
    }

    private static Executor lookupExecutor(
            StreamExecutionEnvironment executionEnvironment, ClassLoader userClassLoader) {
        try {
            final ExecutorFactory executorFactory =
                    FactoryUtil.discoverFactory(
                            userClassLoader,
                            ExecutorFactory.class,
                            ExecutorFactory.DEFAULT_IDENTIFIER);
            final Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", StreamExecutionEnvironment.class);

            return (Executor) createMethod.invoke(executorFactory, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    private StreamExecutionEnvironment createStreamExecutionEnvironment() {
        // We need not different StreamExecutionEnvironments to build and submit flink job,
        // instead we just use StreamExecutionEnvironment#executeAsync(StreamGraph) method
        // to execute existing StreamGraph.
        // This requires StreamExecutionEnvironment to have a full flink configuration.
        return new StreamExecutionEnvironment(new Configuration(sessionConf), userClassloader);
    }

    private static void setResourceDownloadTmpDir(
            Configuration configuration, SessionHandle sessionId) {
        Path path =
                Paths.get(
                        configuration.get(TableConfigOptions.RESOURCES_DOWNLOAD_DIR),
                        String.format("sql-gateway-%s", sessionId));
        // override resource download temp directory
        configuration.set(
                TableConfigOptions.RESOURCES_DOWNLOAD_DIR, path.toAbsolutePath().toString());
    }

    private static ModuleManager buildModuleManager(SessionEnvironment environment) {
        final ModuleManager moduleManager = new ModuleManager();

        environment
                .getRegisteredModules()
                .forEach(
                        (moduleName, module) -> {
                            Deque<String> moduleNames =
                                    new ArrayDeque<>(moduleManager.listModules());
                            moduleNames.addFirst(moduleName);

                            moduleManager.loadModule(moduleName, module);
                            moduleManager.useModules(moduleNames.toArray(new String[0]));
                        });

        return moduleManager;
    }

    private static CatalogManager buildCatalogManager(
            Configuration configuration,
            URLClassLoader userClassLoader,
            SessionEnvironment environment) {
        CatalogManager.Builder builder =
                CatalogManager.newBuilder()
                        // Currently, the classloader is only used by DataTypeFactory.
                        .classLoader(userClassLoader)
                        .config(configuration);

        // init default catalog
        String defaultCatalogName;
        Catalog defaultCatalog;
        if (environment.getDefaultCatalog().isPresent()) {
            defaultCatalogName = environment.getDefaultCatalog().get();
            defaultCatalog = environment.getRegisteredCatalogs().get(defaultCatalogName);
        } else {
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().withConfiguration(configuration).build();
            defaultCatalogName = settings.getBuiltInCatalogName();

            if (environment.getRegisteredCatalogs().containsKey(defaultCatalogName)) {
                throw new SqlGatewayException(
                        String.format(
                                "The name of the registered catalog is conflicts with the built-in default catalog name: %s.",
                                defaultCatalogName));
            }

            defaultCatalog =
                    new GenericInMemoryCatalog(
                            defaultCatalogName, settings.getBuiltInDatabaseName());
        }
        defaultCatalog.open();

        CatalogManager catalogManager =
                builder.defaultCatalog(defaultCatalogName, defaultCatalog).build();

        // filter the default catalog out to avoid repeated registration
        environment
                .getRegisteredCatalogs()
                .forEach(
                        (catalogName, catalog) -> {
                            if (!catalogName.equals(defaultCatalogName)) {
                                catalogManager.registerCatalog(catalogName, catalog);
                            }
                        });

        return catalogManager;
    }

    // --------------------------------------------------------------------------------------------
    // Inner class
    // --------------------------------------------------------------------------------------------

    /** session state. */
    public static class SessionState {

        public final CatalogManager catalogManager;
        public final ResourceManager resourceManager;
        public final FunctionCatalog functionCatalog;
        public final ModuleManager moduleManager;

        public SessionState(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                ResourceManager resourceManager,
                FunctionCatalog functionCatalog) {
            this.catalogManager = catalogManager;
            this.moduleManager = moduleManager;
            this.resourceManager = resourceManager;
            this.functionCatalog = functionCatalog;
        }
    }
}

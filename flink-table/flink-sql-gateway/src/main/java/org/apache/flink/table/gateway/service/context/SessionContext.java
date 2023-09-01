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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogStoreHolder;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
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

    private boolean isStatementSetState;
    private final List<ModifyOperation> statementSetOperations;

    protected SessionContext(
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
        this.isStatementSetState = false;
        this.statementSetOperations = new ArrayList<>();
    }

    // --------------------------------------------------------------------------------------------
    // Getter/Setter
    // --------------------------------------------------------------------------------------------

    public SessionHandle getSessionId() {
        return this.sessionId;
    }

    public Configuration getSessionConf() {
        return new UnmodifiableConfiguration(sessionConf);
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

    public DefaultContext getDefaultContext() {
        return defaultContext;
    }

    public URLClassLoader getUserClassloader() {
        return userClassloader;
    }

    public void set(String key, String value) {
        try {
            // Test whether the key value will influence the creation of the Executor.
            createOperationExecutor(Configuration.fromMap(Collections.singletonMap(key, value)))
                    .getTableEnvironment();
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

    // --------------------------------------------------------------------------------------------
    // Begin statement set
    // --------------------------------------------------------------------------------------------

    public boolean isStatementSetState() {
        return isStatementSetState;
    }

    public void enableStatementSet() {
        isStatementSetState = true;
    }

    public void disableStatementSet() {
        isStatementSetState = false;
        statementSetOperations.clear();
    }

    public List<ModifyOperation> getStatementSetOperations() {
        return Collections.unmodifiableList(new ArrayList<>(statementSetOperations));
    }

    public void addStatementSetOperation(ModifyOperation operation) {
        statementSetOperations.add(operation);
    }

    // --------------------------------------------------------------------------------------------

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
        Configuration configuration =
                initializeConfiguration(defaultContext, environment, sessionId);
        final MutableURLClassLoader userClassLoader =
                FlinkUserCodeClassLoaders.create(
                        defaultContext.getDependencies().toArray(new URL[0]),
                        SessionContext.class.getClassLoader(),
                        configuration);
        final ResourceManager resourceManager = new ResourceManager(configuration, userClassLoader);
        return new SessionContext(
                defaultContext,
                sessionId,
                environment.getSessionEndpointVersion(),
                configuration,
                userClassLoader,
                initializeSessionState(environment, configuration, resourceManager),
                new OperationManager(operationExecutorService));
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------------------------

    protected static Configuration initializeConfiguration(
            DefaultContext defaultContext,
            SessionEnvironment environment,
            SessionHandle sessionId) {
        Configuration configuration = defaultContext.getFlinkConfig().clone();
        configuration.addAll(Configuration.fromMap(environment.getSessionConfig()));
        // every session configure the specific local resource download directory
        Path path =
                Paths.get(
                        configuration.get(TableConfigOptions.RESOURCES_DOWNLOAD_DIR),
                        String.format("sql-gateway-%s", sessionId));
        // override resource download temp directory
        configuration.set(
                TableConfigOptions.RESOURCES_DOWNLOAD_DIR, path.toAbsolutePath().toString());
        return configuration;
    }

    protected static SessionState initializeSessionState(
            SessionEnvironment environment,
            Configuration configuration,
            ResourceManager resourceManager) {
        final ModuleManager moduleManager =
                buildModuleManager(
                        environment, configuration, resourceManager.getUserClassLoader());

        final CatalogManager catalogManager =
                buildCatalogManager(
                        configuration, resourceManager.getUserClassLoader(), environment);

        final FunctionCatalog functionCatalog =
                new FunctionCatalog(configuration, resourceManager, catalogManager, moduleManager);
        return new SessionState(catalogManager, moduleManager, resourceManager, functionCatalog);
    }

    private static ModuleManager buildModuleManager(
            SessionEnvironment environment,
            ReadableConfig readableConfig,
            ClassLoader classLoader) {
        final ModuleManager moduleManager = new ModuleManager();

        environment
                .getRegisteredModuleCreators()
                .forEach(
                        (moduleName, moduleCreator) -> {
                            Deque<String> moduleNames =
                                    new ArrayDeque<>(moduleManager.listModules());
                            moduleNames.addFirst(moduleName);

                            Module module = moduleCreator.create(readableConfig, classLoader);
                            moduleManager.loadModule(moduleName, module);
                            moduleManager.useModules(moduleNames.toArray(new String[0]));
                        });

        return moduleManager;
    }

    private static CatalogManager buildCatalogManager(
            Configuration configuration,
            URLClassLoader userClassLoader,
            SessionEnvironment environment) {

        CatalogStoreFactory catalogStoreFactory =
                TableFactoryUtil.findAndCreateCatalogStoreFactory(configuration, userClassLoader);
        CatalogStoreFactory.Context catalogStoreFactoryContext =
                TableFactoryUtil.buildCatalogStoreFactoryContext(configuration, userClassLoader);
        catalogStoreFactory.open(catalogStoreFactoryContext);
        CatalogStoreHolder catalogStore =
                CatalogStoreHolder.newBuilder()
                        .catalogStore(catalogStoreFactory.createCatalogStore())
                        .classloader(userClassLoader)
                        .config(configuration)
                        .factory(catalogStoreFactory)
                        .build();

        CatalogManager.Builder builder =
                CatalogManager.newBuilder()
                        // Currently, the classloader is only used by DataTypeFactory.
                        .classLoader(userClassLoader)
                        .config(configuration)
                        .catalogModificationListeners(
                                TableFactoryUtil.findCatalogModificationListenerList(
                                        configuration, userClassLoader))
                        .catalogStoreHolder(catalogStore);

        // init default catalog
        String defaultCatalogName;
        Catalog defaultCatalog;
        if (environment.getDefaultCatalog().isPresent()) {
            defaultCatalogName = environment.getDefaultCatalog().get();
            defaultCatalog =
                    environment
                            .getRegisteredCatalogCreators()
                            .get(defaultCatalogName)
                            .create(configuration, userClassLoader);
        } else {
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().withConfiguration(configuration).build();
            defaultCatalogName = settings.getBuiltInCatalogName();

            if (environment.getRegisteredCatalogCreators().containsKey(defaultCatalogName)) {
                throw new SqlGatewayException(
                        String.format(
                                "The name of the registered catalog is conflicts with the built-in default catalog name: %s.",
                                defaultCatalogName));
            }

            defaultCatalog =
                    catalogStore
                            .catalogStore()
                            .getCatalog(defaultCatalogName)
                            .map(
                                    catalogDescriptor ->
                                            FactoryUtil.createCatalog(
                                                    defaultCatalogName,
                                                    catalogDescriptor.getConfiguration().toMap(),
                                                    catalogStore.config(),
                                                    catalogStore.classLoader()))
                            .orElse(
                                    new GenericInMemoryCatalog(
                                            defaultCatalogName, settings.getBuiltInDatabaseName()));
        }
        defaultCatalog.open();

        CatalogManager catalogManager =
                builder.defaultCatalog(defaultCatalogName, defaultCatalog).build();

        // filter the default catalog out to avoid repeated registration
        environment
                .getRegisteredCatalogCreators()
                .forEach(
                        (catalogName, catalogCreator) -> {
                            if (!catalogName.equals(defaultCatalogName)) {
                                catalogManager.registerCatalog(
                                        catalogName,
                                        catalogCreator.create(configuration, userClassLoader));
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

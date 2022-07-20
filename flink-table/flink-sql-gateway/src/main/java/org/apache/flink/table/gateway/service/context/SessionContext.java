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

import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
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
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.module.ModuleManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    /** Close resources, e.g. catalogs. */
    public void close() {
        operationManager.close();

        for (String name : sessionState.catalogManager.listCatalogs()) {
            sessionState.catalogManager.getCatalog(name).ifPresent(Catalog::close);
        }
        try {
            userClassloader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing class loader.", e);
        }
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

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static SessionContext create(
            DefaultContext defaultContext,
            SessionHandle sessionId,
            EndpointVersion endpointVersion,
            Configuration sessionConf,
            ExecutorService operationExecutorService) {
        // --------------------------------------------------------------------------------------------------------------
        // Init config
        // --------------------------------------------------------------------------------------------------------------

        Configuration configuration = defaultContext.getFlinkConfig().clone();
        configuration.addAll(sessionConf);

        // --------------------------------------------------------------------------------------------------------------
        // Init classloader
        // --------------------------------------------------------------------------------------------------------------

        URLClassLoader classLoader =
                buildClassLoader(Collections.emptySet(), Collections.emptySet(), configuration);

        // --------------------------------------------------------------------------------------------------------------
        // Init session state
        // --------------------------------------------------------------------------------------------------------------

        ModuleManager moduleManager = new ModuleManager();

        final EnvironmentSettings settings = EnvironmentSettings.fromConfiguration(configuration);

        CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        // Currently, the classloader is only used by DataTypeFactory.
                        .classLoader(classLoader)
                        .config(configuration)
                        .defaultCatalog(
                                settings.getBuiltInCatalogName(),
                                new GenericInMemoryCatalog(
                                        settings.getBuiltInCatalogName(),
                                        settings.getBuiltInDatabaseName()))
                        .build();

        FunctionCatalog functionCatalog =
                new FunctionCatalog(configuration, catalogManager, moduleManager, classLoader);
        SessionState sessionState =
                new SessionState(catalogManager, moduleManager, functionCatalog);

        return new SessionContext(
                defaultContext,
                sessionId,
                endpointVersion,
                configuration,
                classLoader,
                sessionState,
                new OperationManager(operationExecutorService));
    }

    private static URLClassLoader buildClassLoader(
            Set<URL> envDependencies, Set<URL> userDependencies, Configuration conf) {
        Set<URL> newDependencies = new HashSet<>();
        newDependencies.addAll(envDependencies);
        newDependencies.addAll(userDependencies);

        // override to use SafetyNetWrapperClassLoader
        conf.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, true);

        return ClientUtils.buildUserCodeClassLoader(
                new ArrayList<>(newDependencies),
                Collections.emptyList(),
                SessionContext.class.getClassLoader(),
                conf);
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

        final Executor executor = lookupExecutor(streamExecEnv);
        return createStreamTableEnvironment(
                streamExecEnv,
                settings,
                tableConfig,
                executor,
                sessionState.catalogManager,
                sessionState.moduleManager,
                sessionState.functionCatalog,
                userClassloader);
    }

    public OperationManager getOperationManager() {
        return operationManager;
    }

    private TableEnvironmentInternal createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            TableConfig tableConfig,
            Executor executor,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog,
            ClassLoader userClassLoader) {

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        userClassLoader,
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                functionCatalog,
                tableConfig,
                env,
                planner,
                executor,
                settings.isStreamingMode(),
                userClassLoader);
    }

    private Executor lookupExecutor(StreamExecutionEnvironment executionEnvironment) {
        try {
            final ExecutorFactory executorFactory =
                    FactoryUtil.discoverFactory(
                            userClassloader,
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

    // --------------------------------------------------------------------------------------------
    // Inner class
    // --------------------------------------------------------------------------------------------

    /** session state. */
    public static class SessionState {

        public final CatalogManager catalogManager;
        public final FunctionCatalog functionCatalog;
        public final ModuleManager moduleManager;

        public SessionState(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                FunctionCatalog functionCatalog) {
            this.catalogManager = catalogManager;
            this.moduleManager = moduleManager;
            this.functionCatalog = functionCatalog;
        }
    }
}

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

package org.apache.flink.client.python;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.python.util.PythonDependencyUtils;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalListener;

import py4j.GatewayServer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.client.python.PythonEnvUtils.CHECK_INTERVAL;
import static org.apache.flink.client.python.PythonEnvUtils.PythonProcessShutdownHook;
import static org.apache.flink.client.python.PythonEnvUtils.TIMEOUT_MILLIS;
import static org.apache.flink.client.python.PythonEnvUtils.getGatewayServer;
import static org.apache.flink.client.python.PythonEnvUtils.launchPy4jPythonClient;
import static org.apache.flink.client.python.PythonEnvUtils.maxConcurrentPythonFunctionFactories;
import static org.apache.flink.client.python.PythonEnvUtils.shutdownPythonProcess;
import static org.apache.flink.client.python.PythonEnvUtils.startGatewayServer;

/** The factory which creates the PythonFunction objects from given module name and object name. */
public interface PythonFunctionFactory {

    ScheduledExecutorService CACHE_CLEANUP_EXECUTOR_SERVICE =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("PythonFunctionFactory"));

    AtomicReference<Boolean> CACHE_CLEANUP_EXECUTOR_SERVICE_STARTED = new AtomicReference<>(false);

    LoadingCache<CacheKey, PythonFunctionFactory> PYTHON_FUNCTION_FACTORY_CACHE =
            CacheBuilder.newBuilder()
                    .expireAfterAccess(1, TimeUnit.MINUTES)
                    .maximumSize(maxConcurrentPythonFunctionFactories)
                    .removalListener(
                            (RemovalListener<CacheKey, PythonFunctionFactory>)
                                    removalNotification -> {
                                        if (removalNotification.getValue() instanceof Closeable) {
                                            try {
                                                ((Closeable) removalNotification.getValue())
                                                        .close();
                                            } catch (IOException ignore) {
                                            }
                                        }
                                    })
                    .build(
                            new CacheLoader<CacheKey, PythonFunctionFactory>() {
                                @Override
                                public PythonFunctionFactory load(CacheKey cacheKey) {
                                    try {
                                        return createPythonFunctionFactory(cacheKey.config);
                                    } catch (Throwable t) {
                                        throw new RuntimeException(t);
                                    }
                                }
                            });

    /**
     * Returns PythonFunction according to moduleName and objectName.
     *
     * @param moduleName The module name of the Python UDF.
     * @param objectName The function name / class name of the Python UDF.
     * @return The PythonFunction object which represents the Python UDF.
     */
    PythonFunction getPythonFunction(String moduleName, String objectName);

    /**
     * Returns PythonFunction according to the fully qualified name of the Python UDF i.e
     * ${moduleName}.${functionName} or ${moduleName}.${className}.
     *
     * @param fullyQualifiedName The fully qualified name of the Python UDF.
     * @param config The configuration of python dependencies.
     * @param classLoader The classloader which is used to identify different jobs.
     * @return The PythonFunction object which represents the Python UDF.
     */
    static PythonFunction getPythonFunction(
            String fullyQualifiedName, ReadableConfig config, ClassLoader classLoader)
            throws ExecutionException {
        int splitIndex = fullyQualifiedName.lastIndexOf(".");
        if (splitIndex <= 0) {
            throw new IllegalArgumentException(
                    String.format("The fully qualified name is invalid: '%s'", fullyQualifiedName));
        }
        String moduleName = fullyQualifiedName.substring(0, splitIndex);
        String objectName = fullyQualifiedName.substring(splitIndex + 1);

        Configuration mergedConfig =
                new Configuration(
                        ExecutionEnvironment.getExecutionEnvironment().getConfiguration());
        if (config instanceof TableConfig) {
            PythonDependencyUtils.merge(mergedConfig, ((TableConfig) config).getConfiguration());
        } else {
            PythonDependencyUtils.merge(mergedConfig, (Configuration) config);
        }
        PythonFunctionFactory pythonFunctionFactory =
                PYTHON_FUNCTION_FACTORY_CACHE.get(CacheKey.of(mergedConfig, classLoader));
        ensureCacheCleanupExecutorServiceStarted();
        return pythonFunctionFactory.getPythonFunction(moduleName, objectName);
    }

    static void ensureCacheCleanupExecutorServiceStarted() {
        if (CACHE_CLEANUP_EXECUTOR_SERVICE_STARTED.compareAndSet(false, true)) {
            CACHE_CLEANUP_EXECUTOR_SERVICE.scheduleAtFixedRate(
                    PYTHON_FUNCTION_FACTORY_CACHE::cleanUp, 1, 1, TimeUnit.MINUTES);
        }
    }

    static PythonFunctionFactory createPythonFunctionFactory(ReadableConfig config)
            throws ExecutionException, InterruptedException, IOException {
        Map<String, Object> entryPoint;
        PythonProcessShutdownHook shutdownHook = null;
        if (getGatewayServer() == null) {
            GatewayServer gatewayServer = null;
            Process pythonProcess = null;
            String tmpDir = null;
            try {
                gatewayServer = startGatewayServer();
                List<String> commands = new ArrayList<>();
                commands.add("-m");
                commands.add("pyflink.pyflink_callback_server");
                tmpDir =
                        System.getProperty("java.io.tmpdir")
                                + File.separator
                                + "pyflink"
                                + File.separator
                                + UUID.randomUUID();
                pythonProcess =
                        launchPy4jPythonClient(
                                gatewayServer, config, commands, null, tmpDir, false);
                entryPoint = (Map<String, Object>) gatewayServer.getGateway().getEntryPoint();
                int i = 0;
                while (!entryPoint.containsKey("PythonFunctionFactory")) {
                    if (!pythonProcess.isAlive()) {
                        throw new RuntimeException("Python callback server start failed!");
                    }
                    try {
                        Thread.sleep(CHECK_INTERVAL);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(
                                "Interrupted while waiting for the python process to start.", e);
                    }
                    i++;
                    if (i > TIMEOUT_MILLIS / CHECK_INTERVAL) {
                        throw new RuntimeException("Python callback server start failed!");
                    }
                }
            } catch (Throwable e) {
                try {
                    if (gatewayServer != null) {
                        gatewayServer.shutdown();
                    }
                } catch (Throwable e2) {
                    // ignore, do not swallow the origin exception.
                }
                try {
                    if (pythonProcess != null) {
                        shutdownPythonProcess(pythonProcess, TIMEOUT_MILLIS);
                    }
                } catch (Throwable e3) {
                    // ignore, do not swallow the origin exception.
                }
                if (tmpDir != null) {
                    FileUtils.deleteDirectoryQuietly(new File(tmpDir));
                }

                throw e;
            }
            shutdownHook = new PythonProcessShutdownHook(pythonProcess, gatewayServer, tmpDir);
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } else {
            entryPoint = (Map<String, Object>) getGatewayServer().getGateway().getEntryPoint();
        }

        return new PythonFunctionFactoryImpl(
                (PythonFunctionFactory) entryPoint.get("PythonFunctionFactory"), shutdownHook);
    }

    /** The cache key. It only considers the classloader. */
    class CacheKey {
        private final ReadableConfig config;
        private final ClassLoader classLoader;

        CacheKey(ReadableConfig config, ClassLoader classLoader) {
            this.config = config;
            this.classLoader = classLoader;
        }

        public static CacheKey of(ReadableConfig config, ClassLoader classLoader) {
            return new CacheKey(config, classLoader);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof CacheKey && this.classLoader == ((CacheKey) other).classLoader;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(classLoader);
        }
    }
}

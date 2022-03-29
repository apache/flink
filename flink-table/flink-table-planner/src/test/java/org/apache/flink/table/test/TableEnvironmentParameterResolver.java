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

package org.apache.flink.table.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.stream.Stream;

class TableEnvironmentParameterResolver implements ParameterResolver {

    private static final Class<TableEnvironmentImpl> TABLE_ENVIRONMENT_IMPL =
            TableEnvironmentImpl.class;
    private static final Class<StreamTableEnvironment> JAVA_STREAM_TABLE_ENVIRONMENT =
            StreamTableEnvironment.class;
    private static final Class<org.apache.flink.table.api.bridge.scala.StreamTableEnvironment>
            SCALA_STREAM_TABLE_ENVIRONMENT =
                    org.apache.flink.table.api.bridge.scala.StreamTableEnvironment.class;
    private static final Class<StreamExecutionEnvironment> JAVA_STREAM_EXECUTION_ENVIRONMENT =
            StreamExecutionEnvironment.class;
    private static final Class<org.apache.flink.streaming.api.scala.StreamExecutionEnvironment>
            SCALA_STREAM_EXECUTION_ENVIRONMENT =
                    org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.class;

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(TableEnvironmentParameterResolver.class);

    private final Class<?> defaultInjectedTableEnvironment;
    private final RuntimeExecutionMode defaultExecutionMode;

    TableEnvironmentParameterResolver() {
        this(null, RuntimeExecutionMode.STREAMING);
    }

    TableEnvironmentParameterResolver(
            Class<?> defaultInjectedTableEnvironment, RuntimeExecutionMode defaultExecutionMode) {
        this.defaultInjectedTableEnvironment = defaultInjectedTableEnvironment;
        this.defaultExecutionMode = defaultExecutionMode;
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext) {
        Class<?> paramType = parameterContext.getParameter().getType();
        return Stream.of(
                        TABLE_ENVIRONMENT_IMPL,
                        JAVA_STREAM_TABLE_ENVIRONMENT,
                        SCALA_STREAM_TABLE_ENVIRONMENT,
                        JAVA_STREAM_EXECUTION_ENVIRONMENT,
                        SCALA_STREAM_EXECUTION_ENVIRONMENT)
                .anyMatch(paramType::isAssignableFrom);
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext) {
        Class<?> paramType =
                (defaultInjectedTableEnvironment != null)
                        ? defaultInjectedTableEnvironment
                        : parameterContext.getParameter().getType();

        // Check if the annotation is forcing the injection of StreamTableEnvironment
        boolean isForcingStreamEnvironment =
                parameterContext
                        .findAnnotation(WithTableEnvironment.class)
                        .map(WithTableEnvironment::withDataStream)
                        .orElse(false);

        if (paramType.isAssignableFrom(TABLE_ENVIRONMENT_IMPL) && !isForcingStreamEnvironment) {
            EnvironmentSettings environmentSettings =
                    resolveEnvironmentSettings(parameterContext, extensionContext);
            return extensionContext
                    .getStore(NAMESPACE)
                    .getOrComputeIfAbsent(
                            TABLE_ENVIRONMENT_IMPL.getSimpleName(),
                            k ->
                                    (TableEnvironmentImpl)
                                            TableEnvironment.create(environmentSettings),
                            TABLE_ENVIRONMENT_IMPL);
        } else if (paramType.isAssignableFrom(JAVA_STREAM_TABLE_ENVIRONMENT)
                || isForcingStreamEnvironment) {
            EnvironmentSettings environmentSettings =
                    resolveEnvironmentSettings(parameterContext, extensionContext);
            StreamExecutionEnvironment streamExecutionEnvironment =
                    resolveStreamExecutionEnvironment(extensionContext);
            return extensionContext
                    .getStore(NAMESPACE)
                    .getOrComputeIfAbsent(
                            JAVA_STREAM_TABLE_ENVIRONMENT.getSimpleName(),
                            k ->
                                    StreamTableEnvironment.create(
                                            streamExecutionEnvironment, environmentSettings),
                            JAVA_STREAM_TABLE_ENVIRONMENT);
        } else if (paramType.isAssignableFrom(SCALA_STREAM_TABLE_ENVIRONMENT)) {
            EnvironmentSettings environmentSettings =
                    resolveEnvironmentSettings(parameterContext, extensionContext);
            StreamExecutionEnvironment streamExecutionEnvironment =
                    resolveStreamExecutionEnvironment(extensionContext);
            return extensionContext
                    .getStore(NAMESPACE)
                    .getOrComputeIfAbsent(
                            SCALA_STREAM_TABLE_ENVIRONMENT.getSimpleName(),
                            k ->
                                    org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
                                            .create(
                                                    new org.apache.flink.streaming.api.scala
                                                            .StreamExecutionEnvironment(
                                                            streamExecutionEnvironment),
                                                    environmentSettings),
                            SCALA_STREAM_TABLE_ENVIRONMENT);
        } else if (paramType.isAssignableFrom(JAVA_STREAM_EXECUTION_ENVIRONMENT)) {
            return resolveStreamExecutionEnvironment(extensionContext);
        } else if (paramType.isAssignableFrom(SCALA_STREAM_EXECUTION_ENVIRONMENT)) {
            return new org.apache.flink.streaming.api.scala.StreamExecutionEnvironment(
                    resolveStreamExecutionEnvironment(extensionContext));
        }
        throw new ParameterResolutionException("Unexpected param type " + paramType);
    }

    private EnvironmentSettings resolveEnvironmentSettings(
            ParameterContext parameterContext, ExtensionContext extensionContext) {
        RuntimeExecutionMode executionMode = defaultExecutionMode;
        if (parameterContext.isAnnotated(WithTableEnvironment.class)) {
            executionMode =
                    parameterContext
                            .findAnnotation(WithTableEnvironment.class)
                            .get()
                            .executionMode();
        } else if (extensionContext
                .getRequiredTestMethod()
                .isAnnotationPresent(WithTableEnvironment.class)) {
            executionMode =
                    extensionContext
                            .getRequiredTestMethod()
                            .getAnnotation(WithTableEnvironment.class)
                            .executionMode();
        }
        if (executionMode == RuntimeExecutionMode.AUTOMATIC) {
            throw new IllegalStateException("Unsupported RuntimeExecutionMode.AUTOMATIC");
        }

        return executionMode == RuntimeExecutionMode.STREAMING
                ? EnvironmentSettings.newInstance().inStreamingMode().build()
                : EnvironmentSettings.newInstance().inBatchMode().build();
    }

    private StreamExecutionEnvironment resolveStreamExecutionEnvironment(
            ExtensionContext extensionContext) {
        return extensionContext
                .getStore(NAMESPACE)
                .getOrComputeIfAbsent(
                        StreamExecutionEnvironment.class.getSimpleName(),
                        k -> StreamExecutionEnvironment.getExecutionEnvironment(),
                        StreamExecutionEnvironment.class);
    }
}

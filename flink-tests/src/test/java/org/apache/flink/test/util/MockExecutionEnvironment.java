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

package org.apache.flink.test.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.connector.dsv2.FromDataSource;
import org.apache.flink.api.connector.dsv2.Source;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.connector.datagen.functions.FromElementsGeneratorFunction;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;

import java.util.Collection;
import java.util.Set;

/**
 * Mock implementation of {@link ExecutionEnvironment} that uses {@link MockDataGeneratorSource} in
 * {@link MockExecutionEnvironment#fromSource(Source, String)}.
 */
public class MockExecutionEnvironment extends ExecutionEnvironmentImpl {

    private final WatermarkSupplier watermarkSupplier;

    public MockExecutionEnvironment(
            PipelineExecutorServiceLoader executorServiceLoader,
            Configuration configuration,
            ClassLoader classLoader,
            WatermarkSupplier watermarkSupplier) {
        super(executorServiceLoader, configuration, classLoader);
        this.watermarkSupplier = watermarkSupplier;
    }

    /**
     * Create and return an instance of {@link ExecutionEnvironment}.
     *
     * <p>IMPORTANT: The method is only expected to be called by {@link ExecutionEnvironment} via
     * reflection, so we must ensure that the package path, class name and the signature of this
     * method does not change.
     */
    public static MockExecutionEnvironment newInstance(WatermarkSupplier watermarkSupplier) {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, "local");
        configuration.set(DeploymentOptions.ATTACHED, true);
        return new MockExecutionEnvironment(
                new DefaultExecutorServiceLoader(), configuration, null, watermarkSupplier);
    }

    @Override
    public <OUT> NonKeyedPartitionStream<OUT> fromSource(
            Source<OUT> source,
            String sourceName,
            Set<WatermarkDeclaration> watermarkDeclarations) {
        if (source instanceof FromDataSource) {
            Collection<OUT> data = ((FromDataSource<OUT>) source).getData();
            TypeInformation<OUT> outType = extractTypeInfoFromCollection(data);

            FromElementsGeneratorFunction<OUT> generatorFunction =
                    new FromElementsGeneratorFunction<>(outType, executionConfig, data);

            DataGeneratorSource<OUT> generatorSource =
                    new MockDataGeneratorSource<>(
                            generatorFunction,
                            data.size(),
                            outType,
                            watermarkSupplier,
                            watermarkDeclarations);

            return super.fromSource(
                    new WrappedSource<>(generatorSource),
                    "Collection Source",
                    watermarkDeclarations);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported type of sink, you could use DataStreamV2SourceUtils to wrap a FLIP-27 based source.");
        }
    }
}

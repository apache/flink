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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.WatermarkDeclarable;

import java.util.Collections;
import java.util.Set;

/**
 * Wrapper on top of {@link DataGeneratorSource} to use {@link MockGeneratorSourceReaderFactory}.
 */
@Experimental
public class MockDataGeneratorSource<OUT> extends DataGeneratorSource<OUT>
        implements WatermarkDeclarable {

    private static final long serialVersionUID = 1L;

    private final Set<WatermarkDeclaration> watermarkDeclarations;
    /**
     * Instantiates a new {@code DataGeneratorSource}.
     *
     * @param generatorFunction The {@code GeneratorFunction} function.
     * @param count The number of generated data points.
     * @param typeInfo The type of the produced data points.
     * @param watermarkSupplier Generalized watermark supplier to mock watermark events.
     */
    public MockDataGeneratorSource(
            GeneratorFunction<Long, OUT> generatorFunction,
            long count,
            TypeInformation<OUT> typeInfo,
            WatermarkSupplier watermarkSupplier) {
        this(generatorFunction, count, typeInfo, watermarkSupplier, Collections.emptySet());
    }

    /**
     * Instantiates a new {@code DataGeneratorSource}.
     *
     * @param generatorFunction The {@code GeneratorFunction} function.
     * @param count The number of generated data points.
     * @param typeInfo The type of the produced data points.
     * @param watermarkSupplier Generalized watermark supplier to mock watermark events.
     * @param watermarkDeclarations Watermark Declarations for source.
     */
    public MockDataGeneratorSource(
            GeneratorFunction<Long, OUT> generatorFunction,
            long count,
            TypeInformation<OUT> typeInfo,
            WatermarkSupplier watermarkSupplier,
            Set<WatermarkDeclaration> watermarkDeclarations) {
        super(
                new MockGeneratorSourceReaderFactory<>(
                        generatorFunction, RateLimiterStrategy.noOp(), watermarkSupplier),
                generatorFunction,
                count,
                typeInfo);
        this.watermarkDeclarations = watermarkDeclarations;
    }

    @Override
    public Set<WatermarkDeclaration> watermarkDeclarations() {
        return watermarkDeclarations;
    }
}

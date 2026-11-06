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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.SourceOptions;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import java.util.Locale;

/**
 * Creates the {@link RateLimitCalculator} selected by {@link
 * SourceOptions#SOURCE_DYNAMIC_RATE_LIMIT_CALCULATOR}.
 */
@Internal
public final class RateLimitCalculatorFactory {

    public static final String CHECKPOINT_DURATION = "checkpoint-duration";

    private RateLimitCalculatorFactory() {}

    /** Creates the calculator from the configuration, using the context class loader. */
    public static RateLimitCalculator fromConfiguration(ReadableConfig config) {
        return fromConfiguration(config, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Creates the calculator from the configuration. A custom class is loaded with the given class
     * loader.
     *
     * @throws IllegalArgumentException if the configured value is neither a built-in name nor the
     *     name of an instantiable {@link RateLimitCalculator}.
     */
    public static RateLimitCalculator fromConfiguration(
            ReadableConfig config, ClassLoader classLoader) {
        return create(config.get(SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_CALCULATOR), classLoader);
    }

    /**
     * Creates a calculator from a built-in name ({@value #CHECKPOINT_DURATION}) or the fully
     * qualified name of a {@link RateLimitCalculator} with a public no-arg constructor.
     */
    public static RateLimitCalculator create(String name, ClassLoader classLoader) {
        final String trimmed = name.trim();
        switch (trimmed.toLowerCase(Locale.ROOT)) {
            case CHECKPOINT_DURATION:
                return new CheckpointDurationRateLimitCalculator();
            default:
                try {
                    return InstantiationUtil.instantiate(
                            trimmed, RateLimitCalculator.class, classLoader);
                } catch (FlinkException | RuntimeException e) {
                    throw new IllegalArgumentException(
                            "Invalid value for "
                                    + SourceOptions.SOURCE_DYNAMIC_RATE_LIMIT_CALCULATOR.key()
                                    + ": "
                                    + name,
                            e);
                }
        }
    }
}

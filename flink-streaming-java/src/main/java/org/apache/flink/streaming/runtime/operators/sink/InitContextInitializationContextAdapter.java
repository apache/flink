/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.function.Supplier;

/**
 * Adapter between {@link Sink.InitContext} and {@link SerializationSchema.InitializationContext}.
 */
class InitContextInitializationContextAdapter implements SerializationSchema.InitializationContext {

    private final UserCodeClassLoader userCodeClassLoader;
    private final Supplier<MetricGroup> metricGroupSupplier;
    private MetricGroup cachedMetricGroup;

    public InitContextInitializationContextAdapter(
            UserCodeClassLoader userCodeClassLoader, Supplier<MetricGroup> metricGroupSupplier) {
        this.userCodeClassLoader = userCodeClassLoader;
        this.metricGroupSupplier = metricGroupSupplier;
    }

    @Override
    public MetricGroup getMetricGroup() {
        if (cachedMetricGroup == null) {
            cachedMetricGroup = metricGroupSupplier.get();
        }
        return cachedMetricGroup;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }
}

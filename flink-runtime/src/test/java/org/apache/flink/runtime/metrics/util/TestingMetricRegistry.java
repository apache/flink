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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.util.function.TriConsumer;

/** <code>TestingMetricRegistry</code> is the test implementation for {@link MetricRegistry}. */
public class TestingMetricRegistry implements MetricRegistry {

    private final char delimiter;
    private final int numberReporters;
    private final TriConsumer<Metric, String, AbstractMetricGroup<?>> registerConsumer;
    private final TriConsumer<Metric, String, AbstractMetricGroup<?>> unregisterConsumer;
    private final ScopeFormats scopeFormats;

    private TestingMetricRegistry(
            char delimiter,
            int numberReporters,
            TriConsumer<Metric, String, AbstractMetricGroup<?>> registerConsumer,
            TriConsumer<Metric, String, AbstractMetricGroup<?>> unregisterConsumer,
            ScopeFormats scopeFormats) {
        this.delimiter = delimiter;
        this.numberReporters = numberReporters;
        this.registerConsumer = registerConsumer;
        this.unregisterConsumer = unregisterConsumer;
        this.scopeFormats = scopeFormats;
    }

    @Override
    public char getDelimiter() {
        return delimiter;
    }

    @Override
    public int getNumberReporters() {
        return numberReporters;
    }

    @Override
    public void register(Metric metric, String metricName, AbstractMetricGroup group) {
        registerConsumer.accept(metric, metricName, group);
    }

    @Override
    public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
        unregisterConsumer.accept(metric, metricName, group);
    }

    @Override
    public ScopeFormats getScopeFormats() {
        return scopeFormats;
    }

    public static TestingMetricRegistryBuilder builder() {
        return new TestingMetricRegistryBuilder();
    }

    /** <code>TestingMetricRegistryBuilder</code> builds TestingMetricRegistry instances. */
    public static class TestingMetricRegistryBuilder {

        private char delimiter = '.';
        private int numberReporters = 0;
        private TriConsumer<Metric, String, AbstractMetricGroup<?>> registerConsumer =
                (ignoreMetric, ignoreMetricName, ignoreGroup) -> {};
        private TriConsumer<Metric, String, AbstractMetricGroup<?>> unregisterConsumer =
                (ignoreMetric, ignoreMetricName, ignoreGroup) -> {};
        private ScopeFormats scopeFormats = ScopeFormats.fromConfig(new Configuration());

        private TestingMetricRegistryBuilder() {}

        public TestingMetricRegistryBuilder setDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public TestingMetricRegistryBuilder setNumberReporters(int numberReporters) {
            this.numberReporters = numberReporters;
            return this;
        }

        public TestingMetricRegistryBuilder setRegisterConsumer(
                TriConsumer<Metric, String, AbstractMetricGroup<?>> registerConsumer) {
            this.registerConsumer = registerConsumer;
            return this;
        }

        public TestingMetricRegistryBuilder setUnregisterConsumer(
                TriConsumer<Metric, String, AbstractMetricGroup<?>> unregisterConsumer) {
            this.unregisterConsumer = unregisterConsumer;
            return this;
        }

        public TestingMetricRegistryBuilder setScopeFormats(ScopeFormats scopeFormats) {
            this.scopeFormats = scopeFormats;
            return this;
        }

        public TestingMetricRegistry build() {
            return new TestingMetricRegistry(
                    delimiter, numberReporters, registerConsumer, unregisterConsumer, scopeFormats);
        }
    }
}

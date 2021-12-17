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

package org.apache.flink.metrics.util;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/** A customizable test {@link MetricGroup} implementation. */
public class TestMetricGroup implements MetricGroup, LogicalScopeProvider {

    private final String[] scopeComponents;
    private final Map<String, String> variables;
    private final BiFunction<String, Optional<CharacterFilter>, String> metricIdentifierFunction;
    private final BiFunction<CharacterFilter, Optional<Character>, String> logicalScopeFunction;

    public TestMetricGroup(
            String[] scopeComponents,
            Map<String, String> variables,
            BiFunction<String, Optional<CharacterFilter>, String> metricIdentifierFunction,
            BiFunction<CharacterFilter, Optional<Character>, String> logicalScopeFunction) {
        this.scopeComponents = scopeComponents;
        this.variables = variables;
        this.metricIdentifierFunction = metricIdentifierFunction;
        this.logicalScopeFunction = logicalScopeFunction;
    }

    public static TestMetricGroupBuilder newBuilder() {
        return new TestMetricGroupBuilder();
    }

    @Override
    public Counter counter(String name) {
        return new SimpleCounter();
    }

    @Override
    public <C extends Counter> C counter(String name, C counter) {
        return counter;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
        return gauge;
    }

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
        return histogram;
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        return meter;
    }

    @Override
    public MetricGroup addGroup(String name) {
        return this;
    }

    @Override
    public MetricGroup addGroup(String key, String value) {
        return this;
    }

    @Override
    public String[] getScopeComponents() {
        return scopeComponents;
    }

    @Override
    public Map<String, String> getAllVariables() {
        return variables;
    }

    @Override
    public String getMetricIdentifier(String metricName) {
        return metricIdentifierFunction.apply(metricName, Optional.empty());
    }

    @Override
    public String getMetricIdentifier(String metricName, CharacterFilter filter) {
        return metricIdentifierFunction.apply(metricName, Optional.of(filter));
    }

    @Override
    public String getLogicalScope(CharacterFilter filter) {
        return logicalScopeFunction.apply(filter, Optional.empty());
    }

    @Override
    public String getLogicalScope(CharacterFilter filter, char delimiter) {
        return logicalScopeFunction.apply(filter, Optional.of(delimiter));
    }

    @Override
    public MetricGroup getWrappedMetricGroup() {
        return this;
    }

    /** Builder for {@link TestMetricGroup}. */
    public static final class TestMetricGroupBuilder {
        private String[] scopeComponents = new String[] {};
        private Map<String, String> variables = Collections.emptyMap();
        private BiFunction<String, Optional<CharacterFilter>, String> metricIdentifierFunction =
                (name, filter) -> filter.map(f -> f.filterCharacters(name)).orElse(name);

        private BiFunction<CharacterFilter, Optional<Character>, String> logicalScopeFunction =
                (characterFilter, character) -> "logicalScope";

        public TestMetricGroupBuilder setScopeComponents(String[] scopeComponents) {
            this.scopeComponents = scopeComponents;
            return this;
        }

        public TestMetricGroupBuilder setVariables(Map<String, String> variables) {
            this.variables = variables;
            return this;
        }

        public TestMetricGroupBuilder setMetricIdentifierFunction(
                BiFunction<String, Optional<CharacterFilter>, String> metricIdentifierFunction) {
            this.metricIdentifierFunction = metricIdentifierFunction;
            return this;
        }

        public TestMetricGroupBuilder setLogicalScopeFunction(
                BiFunction<CharacterFilter, Optional<Character>, String> logicalScopeFunction) {
            this.logicalScopeFunction = logicalScopeFunction;
            return this;
        }

        public TestMetricGroup build() {
            return new TestMetricGroup(
                    scopeComponents, variables, metricIdentifierFunction, logicalScopeFunction);
        }
    }
}

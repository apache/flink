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

package org.apache.flink.runtime.failure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricherFactory;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.commons.collections.IteratorUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.failure.FailureEnricherUtils.MERGE_EXCEPTION_MSG;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link FailureEnricherUtils} class. */
@ExtendWith(TestLoggerExtension.class)
class FailureEnricherUtilsTest {

    @Test
    public void testGetIncludedFailureEnrichers() {
        Configuration conf = new Configuration();

        // Disabled feature
        conf.setString(JobManagerOptions.FAILURE_ENRICHERS_LIST, "");
        Set<String> result = FailureEnricherUtils.getIncludedFailureEnrichers(conf);
        assertThat(result).hasSize(0);

        // Single enricher
        conf.setString(JobManagerOptions.FAILURE_ENRICHERS_LIST, "enricher1");
        result = FailureEnricherUtils.getIncludedFailureEnrichers(conf);
        assertThat(result).hasSize(1);
        assertThat(result).contains("enricher1");

        // Multiple enrichers with spaces
        conf.setString(JobManagerOptions.FAILURE_ENRICHERS_LIST, "enricher1, enricher2, enricher3");
        result = FailureEnricherUtils.getIncludedFailureEnrichers(conf);
        assertThat(result).hasSize(3);
        assertThat(result).contains("enricher1", "enricher2", "enricher3");

        // Bad delimiter
        conf.setString(JobManagerOptions.FAILURE_ENRICHERS_LIST, "enricher1. enricher2. enricher3");
        result = FailureEnricherUtils.getIncludedFailureEnrichers(conf);
        assertThat(result).hasSize(1);
        assertThat(result).contains("enricher1. enricher2. enricher3");

        // Multiple enrichers with spaces and empty values
        conf.setString(
                JobManagerOptions.FAILURE_ENRICHERS_LIST, "enricher1, ,enricher2,   enricher3");
        result = FailureEnricherUtils.getIncludedFailureEnrichers(conf);
        assertThat(result).hasSize(3);
        assertThat(result).contains("enricher1", "enricher2", "enricher3");
    }

    @Test
    public void testGetFailureEnrichers() {
        final Configuration configuration = new Configuration();
        final Collection<FailureEnricher> emptyEnrichers =
                FailureEnricherUtils.getFailureEnrichers(configuration, createPluginManager());

        // Empty -> disabled feature
        assertThat(emptyEnrichers).hasSize(0);

        // Invalid Name
        configuration.set(
                JobManagerOptions.FAILURE_ENRICHERS_LIST, FailureEnricherUtilsTest.class.getName());
        final Collection<FailureEnricher> invalidEnrichers =
                FailureEnricherUtils.getFailureEnrichers(configuration, createPluginManager());
        // Excluding failure enricher
        assertThat(invalidEnrichers).hasSize(0);

        // Valid Name plus loading
        configuration.set(JobManagerOptions.FAILURE_ENRICHERS_LIST, TestEnricher.class.getName());
        final Collection<FailureEnricher> enrichers =
                FailureEnricherUtils.getFailureEnrichers(configuration, createPluginManager());
        assertThat(enrichers).hasSize(1);
        // verify that the failure enricher was created and returned
        assertThat(enrichers)
                .satisfiesExactly(
                        enricher -> assertThat(enricher).isInstanceOf(TestEnricher.class));

        // Valid plus Invalid Name combination
        configuration.set(
                JobManagerOptions.FAILURE_ENRICHERS_LIST,
                FailureEnricherUtilsTest.class.getName() + "," + TestEnricher.class.getName());
        final Collection<FailureEnricher> validInvalidEnrichers =
                FailureEnricherUtils.getFailureEnrichers(configuration, createPluginManager());
        assertThat(validInvalidEnrichers).hasSize(1);
        assertThat(validInvalidEnrichers)
                .satisfiesExactly(
                        enricher -> assertThat(enricher).isInstanceOf(TestEnricher.class));
    }

    @Test
    public void testGetValidatedEnrichers() {
        // create two enrichers with non-overlapping keys
        final FailureEnricher firstEnricher = new TestEnricher("key1");
        final FailureEnricher secondEnricher = new TestEnricher("key2");

        final Set<FailureEnricher> enrichers =
                new HashSet<FailureEnricher>() {
                    {
                        add(firstEnricher);
                        add(secondEnricher);
                    }
                };

        final Collection<FailureEnricher> validatedEnrichers =
                FailureEnricherUtils.filterInvalidEnrichers(enrichers);

        // expect both enrichers to be valid
        assertThat(validatedEnrichers).hasSize(2);
        assertThat(validatedEnrichers).contains(firstEnricher, secondEnricher);
    }

    @Test
    public void testValidatedEnrichersWithInvalidEntries() {
        // create two enrichers with overlapping keys and a valid one -- must be different classes
        final FailureEnricher validEnricher = new TestEnricher("validKey");
        final FailureEnricher firstOverlapEnricher = new AnotherTestEnricher("key1", "key2");
        final FailureEnricher secondOverlapEnricher = new AndAnotherTestEnricher("key2", "key3");

        final Set<FailureEnricher> enrichers =
                new HashSet<FailureEnricher>() {
                    {
                        add(validEnricher);
                        add(firstOverlapEnricher);
                        add(secondOverlapEnricher);
                    }
                };

        final Collection<FailureEnricher> validatedEnrichers =
                FailureEnricherUtils.filterInvalidEnrichers(enrichers);
        // Only one enricher is valid
        assertThat(validatedEnrichers).hasSize(1);
    }

    @Test
    public void testLabelFutureWithValidEnricher() {
        // validate labelFailure by enricher with correct outputKeys
        final Throwable cause = new RuntimeException("test exception");
        final Set<FailureEnricher> failureEnrichers = new HashSet<>();
        final FailureEnricher validEnricher = new TestEnricher("enricherKey");
        failureEnrichers.add(validEnricher);

        final CompletableFuture<Map<String, String>> result =
                FailureEnricherUtils.labelFailure(
                        cause,
                        null,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        failureEnrichers);

        assertThatFuture(result)
                .eventuallySucceeds()
                .satisfies(
                        labels -> {
                            assertThat(labels).hasSize(1);
                            assertThat(labels).containsKey("enricherKey");
                            assertThat(labels).containsValue("enricherKeyValue");
                        });
    }

    @Test
    public void testLabelFailureWithInvalidEnricher() {
        // validate labelFailure by enricher with wrong outputKeys
        final Throwable cause = new RuntimeException("test exception");
        final String invalidEnricherKey = "invalidKey";
        final Set<FailureEnricher> failureEnrichers = new HashSet<>();
        final FailureEnricher invalidEnricher =
                new TestEnricher(
                        Collections.singletonMap(invalidEnricherKey, "enricherValue"),
                        "enricherKey");
        failureEnrichers.add(invalidEnricher);

        final CompletableFuture<Map<String, String>> result =
                FailureEnricherUtils.labelFailure(
                        cause,
                        null,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        failureEnrichers);
        // Ignoring labels
        assertThatFuture(result).eventuallySucceeds().satisfies(labels -> labels.isEmpty());
    }

    @Test
    public void testLabelFailureWithValidAndThrowingEnricher() {
        // A failing enricher shouldn't affect remaining enrichers with valid labels
        final Throwable cause = new RuntimeException("test exception");
        final FailureEnricher validEnricher = new TestEnricher("enricherKey");
        final FailureEnricher throwingEnricher = new ThrowingEnricher("throwingKey");

        final Set<FailureEnricher> enrichers =
                new HashSet<FailureEnricher>() {
                    {
                        add(validEnricher);
                        add(throwingEnricher);
                    }
                };

        final CompletableFuture<Map<String, String>> result =
                FailureEnricherUtils.labelFailure(
                        cause,
                        null,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        enrichers);

        assertThatFuture(result)
                .eventuallySucceeds()
                .satisfies(
                        labels -> {
                            assertThat(labels).hasSize(1);
                            assertThat(labels).containsKey("enricherKey");
                            assertThat(labels).containsValue("enricherKeyValue");
                        });
    }

    @Test
    public void testLabelFailureMergeException() {
        // Throwing exception labelFailure when merging duplicate keys
        final Throwable cause = new RuntimeException("test failure");
        final FailureEnricher firstEnricher = new TestEnricher("key1", "key2");
        final FailureEnricher secondEnricher = new TestEnricher("key2", "key3");
        final Set<FailureEnricher> enrichers =
                new HashSet<FailureEnricher>() {
                    {
                        add(firstEnricher);
                        add(secondEnricher);
                    }
                };

        final CompletableFuture<Map<String, String>> result =
                FailureEnricherUtils.labelFailure(
                        cause,
                        null,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        enrichers);

        try {
            result.get();
        } catch (Exception e) {
            assertThat(e).hasMessageContaining(String.format(MERGE_EXCEPTION_MSG, "key2"));
        }
    }

    /**
     * Testing plugin manager for {@link FailureEnricherFactory} utilizing {@link
     * TestFailureEnricherFactory}.
     *
     * @return the testing PluginManager
     */
    private static PluginManager createPluginManager() {
        final Map<Class<?>, Iterator<?>> plugins = new HashMap<>();
        plugins.put(
                FailureEnricherFactory.class,
                IteratorUtils.singletonIterator(new TestFailureEnricherFactory()));
        return new TestingPluginManager(plugins);
    }

    /** Factory implementation of {@link TestEnricher} used for plugin load testing. */
    private static class TestFailureEnricherFactory implements FailureEnricherFactory {

        @Override
        public FailureEnricher createFailureEnricher(Configuration conf) {
            return new TestEnricher();
        }
    }

    private static class ThrowingEnricher extends TestEnricher {
        ThrowingEnricher(String... outputKeys) {
            super(outputKeys);
        }

        @Override
        public CompletableFuture<Map<String, String>> processFailure(
                Throwable cause, Context context) {
            final CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("test failure"));
            return future;
        }
    }

    private static class AndAnotherTestEnricher extends TestEnricher {
        AndAnotherTestEnricher(String... outputKeys) {
            super(outputKeys);
        }
    }

    private static class AnotherTestEnricher extends TestEnricher {
        AnotherTestEnricher(String... outputKeys) {
            super(outputKeys);
        }
    }

    private static class TestEnricher implements FailureEnricher {
        private final Set<String> outputKeys;
        private final Map<String, String> outputMap;

        TestEnricher(String... outputKeys) {
            this.outputKeys = Arrays.stream(outputKeys).collect(Collectors.toSet());
            this.outputMap = new HashMap<>();
            this.outputKeys.forEach(key -> outputMap.put(key, key + "Value"));
        }

        TestEnricher(Map<String, String> outputValues, String... outputKeys) {
            this.outputKeys = Arrays.stream(outputKeys).collect(Collectors.toSet());
            this.outputMap = outputValues;
        }

        @Override
        public Set<String> getOutputKeys() {
            return outputKeys;
        }

        @Override
        public CompletableFuture<Map<String, String>> processFailure(
                Throwable cause, Context context) {
            return CompletableFuture.completedFuture(outputMap);
        }
    }
}

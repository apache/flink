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

package org.apache.flink.core.execution;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JobStatusChangedListenerUtils}. */
class JobStatusChangedListenerUtilsTest {

    @AfterEach
    void clearCache() {
        JobStatusChangedListenerUtils.clearListenerCache();
    }

    @Test
    void testEmptyListReturnedWhenNotConfigured() {
        List<JobStatusChangedListener> listeners =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        getClass().getClassLoader(), new Configuration(), Runnable::run);

        assertThat(listeners).isEmpty();
    }

    @Test
    void testListenerCreatedFromConfiguredFactory() {
        Configuration configuration = configWithFactories(TestListenerFactory.class);

        List<JobStatusChangedListener> listeners =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        getClass().getClassLoader(), configuration, Runnable::run);

        assertThat(listeners).hasSize(1).allMatch(l -> l instanceof TestListener);
    }

    @Test
    void testSameListenerInstanceReturnedAcrossCalls() {
        Configuration configuration = configWithFactories(TestListenerFactory.class);
        ClassLoader classLoader = getClass().getClassLoader();

        List<JobStatusChangedListener> first =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        classLoader, configuration, Runnable::run);
        List<JobStatusChangedListener> second =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        classLoader, configuration, Runnable::run);

        assertThat(first.get(0)).isSameAs(second.get(0));
    }

    @Test
    void testDifferentFactoryClassesReturnDifferentInstances() {
        Configuration configuration =
                configWithFactories(TestListenerFactory.class, AnotherTestListenerFactory.class);

        List<JobStatusChangedListener> listeners =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        getClass().getClassLoader(), configuration, Runnable::run);

        assertThat(listeners).hasSize(2);
        assertThat(listeners.get(0)).isNotSameAs(listeners.get(1));
        assertThat(listeners.get(0)).isInstanceOf(TestListener.class);
        assertThat(listeners.get(1)).isInstanceOf(AnotherTestListener.class);
    }

    private static Configuration configWithFactories(Class<?>... factoryClasses) {
        Configuration configuration = new Configuration();
        String[] names = Arrays.stream(factoryClasses).map(Class::getName).toArray(String[]::new);
        configuration.set(JOB_STATUS_CHANGED_LISTENERS, Arrays.asList(names));
        return configuration;
    }

    // -------------------------------------------------------------------------
    // Test factory / listener implementations
    // -------------------------------------------------------------------------

    /** First test listener. */
    public static class TestListener implements JobStatusChangedListener {
        @Override
        public void onEvent(JobStatusChangedEvent event) {}
    }

    /** Factory that creates {@link TestListener}. */
    public static class TestListenerFactory implements JobStatusChangedListenerFactory {
        @Override
        public JobStatusChangedListener createListener(Context context) {
            return new TestListener();
        }
    }

    /** Second test listener, distinct from {@link TestListener}. */
    public static class AnotherTestListener implements JobStatusChangedListener {
        @Override
        public void onEvent(JobStatusChangedEvent event) {}
    }

    /** Factory that creates {@link AnotherTestListener}. */
    public static class AnotherTestListenerFactory implements JobStatusChangedListenerFactory {
        @Override
        public JobStatusChangedListener createListener(Context context) {
            return new AnotherTestListener();
        }
    }
}

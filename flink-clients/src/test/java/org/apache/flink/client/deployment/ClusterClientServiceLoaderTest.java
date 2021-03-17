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

package org.apache.flink.client.deployment;

import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link DefaultClusterClientServiceLoader}. */
public class ClusterClientServiceLoaderTest {

    private static final String VALID_TARGET = "existing";
    private static final String AMBIGUOUS_TARGET = "duplicate";
    private static final String NON_EXISTING_TARGET = "non-existing";

    private static final int VALID_ID = 42;

    private ClusterClientServiceLoader serviceLoaderUnderTest;

    @Before
    public void init() {
        serviceLoaderUnderTest = new DefaultClusterClientServiceLoader();
    }

    @Test
    public void testStandaloneClusterClientFactoryDiscovery() {
        final Configuration config = new Configuration();
        config.setString(DeploymentOptions.TARGET, RemoteExecutor.NAME);

        ClusterClientFactory<StandaloneClusterId> factory =
                serviceLoaderUnderTest.getClusterClientFactory(config);
        assertTrue(factory instanceof StandaloneClientFactory);
    }

    @Test
    public void testFactoryDiscovery() {
        final Configuration config = new Configuration();
        config.setString(DeploymentOptions.TARGET, VALID_TARGET);

        final ClusterClientFactory<Integer> factory =
                serviceLoaderUnderTest.getClusterClientFactory(config);
        assertNotNull(factory);

        final Integer id = factory.getClusterId(config);
        assertThat(id, allOf(is(notNullValue()), equalTo(VALID_ID)));
    }

    @Test(expected = IllegalStateException.class)
    public void testMoreThanOneCompatibleFactoriesException() {
        final Configuration config = new Configuration();
        config.setString(DeploymentOptions.TARGET, AMBIGUOUS_TARGET);

        serviceLoaderUnderTest.getClusterClientFactory(config);
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testNoFactoriesFound() {
        final Configuration config = new Configuration();
        config.setString(DeploymentOptions.TARGET, NON_EXISTING_TARGET);

        final ClusterClientFactory<Integer> factory =
                serviceLoaderUnderTest.getClusterClientFactory(config);
    }

    /** Test {@link ClusterClientFactory} that is successfully discovered. */
    public static class ValidClusterClientFactory extends BaseTestingClusterClientFactory {

        public static final String ID = VALID_TARGET;

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            return configuration.getString(DeploymentOptions.TARGET).equals(VALID_TARGET);
        }

        @Nullable
        @Override
        public Integer getClusterId(Configuration configuration) {
            return VALID_ID;
        }
    }

    /** Test {@link ClusterClientFactory} that has a duplicate. */
    public static class FirstCollidingClusterClientFactory extends BaseTestingClusterClientFactory {

        public static final String ID = AMBIGUOUS_TARGET;

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            return configuration.getString(DeploymentOptions.TARGET).equals(AMBIGUOUS_TARGET);
        }
    }

    /** Test {@link ClusterClientFactory} that has a duplicate. */
    public static class SecondCollidingClusterClientFactory
            extends BaseTestingClusterClientFactory {

        public static final String ID = AMBIGUOUS_TARGET;

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            return configuration.getString(DeploymentOptions.TARGET).equals(AMBIGUOUS_TARGET);
        }
    }

    /**
     * A base test {@link ClusterClientFactory} that supports no operation and is meant to be
     * extended.
     */
    public static class BaseTestingClusterClientFactory implements ClusterClientFactory<Integer> {

        @Override
        public boolean isCompatibleWith(Configuration configuration) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusterDescriptor<Integer> createClusterDescriptor(Configuration configuration) {
            throw new UnsupportedOperationException();
        }

        @Nullable
        @Override
        public Integer getClusterId(Configuration configuration) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ClusterSpecification getClusterSpecification(Configuration configuration) {
            throw new UnsupportedOperationException();
        }
    }
}

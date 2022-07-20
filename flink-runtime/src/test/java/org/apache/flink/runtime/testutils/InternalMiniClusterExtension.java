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

package org.apache.flink.runtime.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.test.junit5.InjectClusterClientConfiguration;
import org.apache.flink.test.junit5.InjectClusterRESTAddress;
import org.apache.flink.test.junit5.InjectMiniCluster;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.net.URI;

/**
 * An extension which starts a {@link MiniCluster} for testing purposes.
 *
 * <p>This should only be used by tests within the flink-runtime module. Other modules should use
 * {@code MiniClusterExtension} provided by flink-test-utils module.
 */
@Internal
public class InternalMiniClusterExtension
        implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private final MiniClusterResource miniClusterResource;

    public InternalMiniClusterExtension(
            final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
        this.miniClusterResource = new MiniClusterResource(miniClusterResourceConfiguration);
    }

    public int getNumberSlots() {
        return miniClusterResource.getNumberSlots();
    }

    public MiniCluster getMiniCluster() {
        return miniClusterResource.getMiniCluster();
    }

    public UnmodifiableConfiguration getClientConfiguration() {
        return miniClusterResource.getClientConfiguration();
    }

    public URI getRestAddres() {
        return miniClusterResource.getRestAddres();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        miniClusterResource.before();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        miniClusterResource.after();
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> parameterType = parameterContext.getParameter().getType();
        if (parameterContext.isAnnotated(InjectMiniCluster.class)
                && parameterType.isAssignableFrom(MiniCluster.class)) {
            return true;
        }
        if (parameterContext.isAnnotated(InjectClusterClientConfiguration.class)
                && parameterType.isAssignableFrom(UnmodifiableConfiguration.class)) {
            return true;
        }
        return parameterContext.isAnnotated(InjectClusterRESTAddress.class)
                && parameterType.isAssignableFrom(URI.class);
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (parameterContext.isAnnotated(InjectMiniCluster.class)) {
            return miniClusterResource.getMiniCluster();
        }
        if (parameterContext.isAnnotated(InjectClusterClientConfiguration.class)) {
            return miniClusterResource.getClientConfiguration();
        }
        if (parameterContext.isAnnotated(InjectClusterRESTAddress.class)) {
            return miniClusterResource.getRestAddres();
        }
        throw new ParameterResolutionException("Unsupported parameter");
    }
}

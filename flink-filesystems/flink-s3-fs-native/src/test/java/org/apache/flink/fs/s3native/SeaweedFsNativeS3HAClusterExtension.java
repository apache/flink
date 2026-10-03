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

package org.apache.flink.fs.s3native;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.function.Function;

/**
 * Bundles a {@link SeaweedFsNativeS3TestContainer} and a {@link MiniClusterExtension} configured to
 * use it, so that HA IT cases backed by the native S3 FS don't each have to wire up and order the
 * two extensions themselves.
 *
 * <p>The {@link MiniClusterExtension} is created lazily in {@link #beforeAll} (after the container
 * is up), so JUnit never discovers it as a registered extension on its own. This class therefore
 * implements the callback/resolver interfaces itself and delegates to the inner {@link
 * MiniClusterExtension}, so parameter injection (e.g. {@code @InjectMiniCluster}) and the per-test
 * lifecycle still work.
 */
final class SeaweedFsNativeS3HAClusterExtension
        implements BeforeAllCallback,
                AfterAllCallback,
                BeforeEachCallback,
                AfterEachCallback,
                ParameterResolver {

    private final AllCallbackWrapper<TestContainerExtension<SeaweedFsNativeS3TestContainer>>
            seaweedFsExtension =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(SeaweedFsNativeS3TestContainer::new));

    private final Function<SeaweedFsNativeS3TestContainer, Configuration> configurationFactory;

    private MiniClusterExtension miniClusterExtension;

    SeaweedFsNativeS3HAClusterExtension(
            Function<SeaweedFsNativeS3TestContainer, Configuration> configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    SeaweedFsNativeS3TestContainer getContainer() {
        return seaweedFsExtension.getCustomExtension().getTestContainer();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        seaweedFsExtension.beforeAll(context);
        miniClusterExtension =
                new MiniClusterExtension(
                        () -> {
                            final Configuration configuration =
                                    configurationFactory.apply(getContainer());
                            FileSystem.initialize(configuration, null);
                            return new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(configuration)
                                    .build();
                        });
        miniClusterExtension.beforeAll(context);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        try {
            if (miniClusterExtension != null) {
                miniClusterExtension.afterAll(context);
            }
        } finally {
            seaweedFsExtension.afterAll(context);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        miniClusterExtension.beforeEach(context);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        miniClusterExtension.afterEach(context);
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return miniClusterExtension.supportsParameter(parameterContext, extensionContext);
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return miniClusterExtension.resolveParameter(parameterContext, extensionContext);
    }
}

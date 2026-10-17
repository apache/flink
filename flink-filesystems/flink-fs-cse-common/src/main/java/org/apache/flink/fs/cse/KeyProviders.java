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

package org.apache.flink.fs.cse;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;

/**
 * Utility for instantiating {@link KeyProvider} instances via {@link KeyProviderFactory}.
 *
 * <p>Filesystem plugins call {@link #instantiate(String, ReadableConfig)} with the factory class
 * name from {@link CseOptions#KEY_PROVIDER_FACTORY_CLASS} and the filesystem configuration.
 */
@Internal
@Immutable
public final class KeyProviders {

    /**
     * Reflectively instantiates a {@link KeyProviderFactory} and uses it to create a {@link
     * KeyProvider}.
     *
     * @param factoryClassName fully-qualified class name of the {@link KeyProviderFactory}
     * @param config the filesystem configuration passed to {@link
     *     KeyProviderFactory#createKeyProvider}
     * @return a new {@link KeyProvider} instance
     * @throws IOException if the factory cannot be found, instantiated, or fails to create the
     *     provider
     */
    public static KeyProvider instantiate(
            final String factoryClassName, final ReadableConfig config) throws IOException {
        try {
            final KeyProviderFactory factory =
                    InstantiationUtil.instantiate(
                            factoryClassName.strip(),
                            KeyProviderFactory.class,
                            Thread.currentThread().getContextClassLoader());
            return factory.createKeyProvider(config);
        } catch (FlinkException | RuntimeException e) {
            throw new IOException(
                    "Failed to instantiate KeyProviderFactory class '" + factoryClassName + "'", e);
        }
    }

    private KeyProviders() {}
}

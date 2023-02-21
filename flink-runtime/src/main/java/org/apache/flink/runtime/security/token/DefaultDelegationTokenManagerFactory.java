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

package org.apache.flink.runtime.security.token;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/** A factory for {@link DefaultDelegationTokenManager}. */
@Internal
public class DefaultDelegationTokenManagerFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultDelegationTokenManagerFactory.class);

    public static DelegationTokenManager create(
            Configuration configuration,
            @Nullable PluginManager pluginManager,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable ExecutorService ioExecutor)
            throws IOException {

        if (configuration.getBoolean(SecurityOptions.DELEGATION_TOKENS_ENABLED)) {
            return new DefaultDelegationTokenManager(
                    configuration, pluginManager, scheduledExecutor, ioExecutor);
        } else {
            return new NoOpDelegationTokenManager();
        }
    }
}

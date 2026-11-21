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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigurationUtils.getBooleanConfigOption;
import static org.apache.flink.core.security.token.DelegationTokenProvider.CONFIG_PREFIX;

/**
 * A {@link DefaultDelegationTokenManager} on top of a {@link MockTokenService} with the executors
 * under the control of the test: an obtain runs when the test says that it is due.
 *
 * <p>The mock provider and receiver are injected through a plugin manager and not registered as
 * services, because other tests count the registered ones.
 */
public final class DelegationTokenManagerTestHarness {

    private final MockDelegationTokenProvider provider;
    private final MockDelegationTokenReceiver receiver = new MockDelegationTokenReceiver();
    private final ManuallyTriggeredScheduledExecutor scheduledExecutor =
            new ManuallyTriggeredScheduledExecutor();
    private final ManuallyTriggeredScheduledExecutorService ioExecutor =
            new ManuallyTriggeredScheduledExecutorService();
    private final DefaultDelegationTokenManager manager;

    /** The manager uses the clock of the token service, so both agree on when a token expires. */
    public DelegationTokenManagerTestHarness(MockTokenService tokenService) {
        provider = new MockDelegationTokenProvider(tokenService);

        final Map<Class<?>, Iterator<?>> plugins = new HashMap<>();
        plugins.put(DelegationTokenProvider.class, Collections.singletonList(provider).iterator());
        plugins.put(DelegationTokenReceiver.class, Collections.singletonList(receiver).iterator());
        final PluginManager pluginManager = new TestingPluginManager(plugins);

        final Configuration configuration = new Configuration();
        configuration.set(getBooleanConfigOption(CONFIG_PREFIX + ".throw.enabled"), false);
        manager =
                new DefaultDelegationTokenManager(
                        configuration,
                        pluginManager,
                        scheduledExecutor,
                        ioExecutor,
                        tokenService.clock());
    }

    public DefaultDelegationTokenManager manager() {
        return manager;
    }

    public MockDelegationTokenProvider provider() {
        return provider;
    }

    public MockDelegationTokenReceiver receiver() {
        return receiver;
    }

    /** The timer of the next obtain fires and the obtain it hands to the IO executor runs. */
    public void obtainIsDue() {
        scheduledExecutor.triggerScheduledTasks();
        ioExecutor.triggerAll();
    }

    /** The delays of the obtains which are waiting, one for each chain of obtains. */
    public List<Long> pendingObtainDelaysMillis() {
        return scheduledExecutor.getActiveScheduledTasks().stream()
                .map(obtain -> obtain.getDelay(TimeUnit.MILLISECONDS))
                .collect(Collectors.toList());
    }
}

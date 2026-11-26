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
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link DelegationTokenProvider} which gets its tokens from a {@link MockTokenService}.
 *
 * <p>It is an instance and not a service-loaded class, so a test holds the instance it injects
 * through a {@code TestingPluginManager} and sees every call.
 */
public final class MockDelegationTokenProvider implements DelegationTokenProvider {

    private final MockTokenService service;
    private final AtomicInteger obtainCalls = new AtomicInteger();
    private volatile boolean tokensRequired = true;
    @Nullable private volatile RuntimeException failure;
    private volatile ThrowingRunnable<Exception> duringObtain = () -> {};

    public MockDelegationTokenProvider(MockTokenService service) {
        this.service = service;
    }

    /** The number of times the token service was asked for tokens, failed attempts included. */
    public int obtainCalls() {
        return obtainCalls.get();
    }

    public void setTokensRequired(boolean tokensRequired) {
        this.tokensRequired = tokensRequired;
    }

    /** Makes the following attempts fail, {@code null} makes them succeed again. */
    public void failWith(@Nullable RuntimeException failure) {
        this.failure = failure;
    }

    /** Runs the action in every attempt after it is counted and before it succeeds or fails. */
    public void duringObtain(ThrowingRunnable<Exception> action) {
        this.duringObtain = action;
    }

    @Override
    public String serviceName() {
        return MockTokenService.SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {}

    @Override
    public boolean delegationTokensRequired() {
        return tokensRequired;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws Exception {
        obtainCalls.incrementAndGet();
        duringObtain.run();
        final RuntimeException currentFailure = failure;
        if (currentFailure != null) {
            throw currentFailure;
        }
        return service.issue();
    }
}

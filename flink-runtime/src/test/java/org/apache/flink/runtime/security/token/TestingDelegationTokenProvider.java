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

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link DelegationTokenProvider} for tests which need several providers with their own behavior.
 * The tokens it hands out are the bytes of {@code "tokens of <service name>"}, so a test can see
 * which provider they came from.
 *
 * <p>It is an instance and not a service-loaded class, so a test injects it through a {@code
 * TestingPluginManager} and sees every call.
 */
public final class TestingDelegationTokenProvider implements DelegationTokenProvider {

    private final String serviceName;
    private final Optional<Long> validUntil;
    private final AtomicInteger obtainCalls = new AtomicInteger();
    private boolean tokensRequired = true;
    private boolean returnNoTokens;
    @Nullable private Throwable initFailure;

    private TestingDelegationTokenProvider(String serviceName, Optional<Long> validUntil) {
        this.serviceName = serviceName;
        this.validUntil = validUntil;
    }

    /** A provider whose tokens are valid until the given time. */
    public static TestingDelegationTokenProvider validUntil(String serviceName, long validUntil) {
        return new TestingDelegationTokenProvider(serviceName, Optional.of(validUntil));
    }

    /** A provider which does not know when its tokens expire. */
    public static TestingDelegationTokenProvider withoutExpiry(String serviceName) {
        return new TestingDelegationTokenProvider(serviceName, Optional.empty());
    }

    /** The provider says that the service needs no tokens. */
    public TestingDelegationTokenProvider notRequiringTokens() {
        tokensRequired = false;
        return this;
    }

    /** Obtaining returns {@code null} instead of tokens, which a provider must never do. */
    public TestingDelegationTokenProvider returningNoTokens() {
        returnNoTokens = true;
        return this;
    }

    /** The init of the provider fails with the given exception or error. */
    public TestingDelegationTokenProvider failingInitWith(Throwable failure) {
        initFailure = failure;
        return this;
    }

    /** The tokens this provider hands out. */
    public byte[] tokens() {
        return ("tokens of " + serviceName).getBytes(StandardCharsets.UTF_8);
    }

    /** The number of times the tokens of this provider were obtained. */
    public int obtainCalls() {
        return obtainCalls.get();
    }

    @Override
    public String serviceName() {
        return serviceName;
    }

    @Override
    public void init(Configuration configuration) throws Exception {
        if (initFailure instanceof Error) {
            throw (Error) initFailure;
        } else if (initFailure instanceof Exception) {
            throw (Exception) initFailure;
        }
    }

    @Override
    public boolean delegationTokensRequired() {
        return tokensRequired;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() {
        obtainCalls.incrementAndGet();
        return returnNoTokens ? null : new ObtainedDelegationTokens(tokens(), validUntil);
    }
}

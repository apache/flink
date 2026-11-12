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

import org.apache.flink.core.security.token.DelegationTokenProvider.ObtainedDelegationTokens;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A fake remote token service. Every issued token is a running generation number, which makes it
 * visible in the tests which tokens a component got. Tokens can be expired on command, so that a
 * test does not have to wait for the lifetime of a token to pass.
 */
public final class MockTokenService {

    public static final String SERVICE_NAME = "mock";

    private final Clock clock;
    private final Duration tokenLifetime;
    private final AtomicInteger lastIssuedGeneration = new AtomicInteger();
    private volatile int minValidGeneration = 1;

    public MockTokenService() {
        this(Clock.systemDefaultZone(), Duration.ofHours(1));
    }

    /**
     * Creates a service whose tokens are valid for the given lifetime.
     *
     * @param clock the notion of now of the service, it decides when an issued token expires
     * @param tokenLifetime the validity of a token counted from its issue
     */
    public MockTokenService(Clock clock, Duration tokenLifetime) {
        this.clock = clock;
        this.tokenLifetime = tokenLifetime;
    }

    public Clock clock() {
        return clock;
    }

    /** Issues the token of the next generation. */
    public ObtainedDelegationTokens issue() {
        final int generation = lastIssuedGeneration.incrementAndGet();
        return new ObtainedDelegationTokens(
                ByteBuffer.allocate(Integer.BYTES).putInt(generation).array(),
                Optional.of(clock.millis() + tokenLifetime.toMillis()));
    }

    /** Simulates that time passed: all tokens issued so far are expired from now on. */
    public void expireEverythingIssuedSoFar() {
        minValidGeneration = lastIssuedGeneration.get() + 1;
    }

    public boolean isValid(int generation) {
        return generation >= minValidGeneration;
    }

    /** Fails the way the real service fails when it is used with an expired token. */
    public void requireValid(int generation) throws IOException {
        if (!isValid(generation)) {
            throw new IOException(
                    "Token generation "
                            + generation
                            + " is expired, the minimum valid generation is "
                            + minValidGeneration);
        }
    }

    /** The generation of a token as the receivers get it. */
    static int generationOf(byte[] token) {
        return ByteBuffer.wrap(token).getInt();
    }

    /** The generation of the token inside the serialized container the listeners get. */
    public static int generationIn(byte[] serializedContainer) throws Exception {
        final DelegationTokenContainer container =
                InstantiationUtil.deserializeObject(
                        serializedContainer, DelegationTokenContainer.class.getClassLoader());
        final byte[] token = container.getTokens().get(SERVICE_NAME);
        checkState(token != null, "The container holds no token of the %s service", SERVICE_NAME);
        return generationOf(token);
    }
}

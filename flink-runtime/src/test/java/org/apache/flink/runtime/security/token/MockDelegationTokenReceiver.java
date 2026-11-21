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
import org.apache.flink.core.security.token.DelegationTokenReceiver;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link DelegationTokenReceiver} which holds the newest token of the {@link MockTokenService}
 * the way a file system holds its credentials, and remembers every token it ever got.
 */
public final class MockDelegationTokenReceiver implements DelegationTokenReceiver {

    private final List<Integer> receivedGenerations = new CopyOnWriteArrayList<>();
    private volatile int heldGeneration;

    /** The generations of the received tokens in the order they arrived. */
    public List<Integer> receivedGenerations() {
        return receivedGenerations;
    }

    /** The generation of the token held right now, 0 if none arrived yet. */
    public int heldGeneration() {
        return heldGeneration;
    }

    @Override
    public String serviceName() {
        return MockTokenService.SERVICE_NAME;
    }

    @Override
    public void init(Configuration configuration) {}

    @Override
    public void onNewTokensObtained(byte[] tokens) {
        final int generation = MockTokenService.generationOf(tokens);
        receivedGenerations.add(generation);
        heldGeneration = generation;
    }
}

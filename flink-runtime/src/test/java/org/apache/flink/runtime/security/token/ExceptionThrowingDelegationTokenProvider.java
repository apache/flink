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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenManagerCallback;
import org.apache.flink.core.security.token.DelegationTokenProvider;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * An example implementation of {@link DelegationTokenProvider} which throws exception when enabled.
 */
public class ExceptionThrowingDelegationTokenProvider implements DelegationTokenProvider {

    public static volatile ThreadLocal<Boolean> throwInInit =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> throwInUsage =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> addToken =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> constructed =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> shouldReobtainOnRegister =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> throwInRegister =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> throwErrorInRegister =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> throwInUnregister =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> throwErrorInUnregister =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Boolean> stopped =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    public static volatile ThreadLocal<Set<JobID>> registeredJobs =
            ThreadLocal.withInitial(HashSet::new);

    public static void reset() {
        throwInInit.set(false);
        throwInUsage.set(false);
        addToken.set(false);
        constructed.set(false);
        shouldReobtainOnRegister.set(false);
        throwInRegister.set(false);
        throwErrorInRegister.set(false);
        throwInUnregister.set(false);
        throwErrorInUnregister.set(false);
        stopped.set(false);
        registeredJobs.get().clear();
    }

    private DelegationTokenManagerCallback callback;

    public ExceptionThrowingDelegationTokenProvider() {
        constructed.set(true);
    }

    @Override
    public String serviceName() {
        return "throw";
    }

    @Override
    public void init(Configuration configuration) {
        if (throwInInit.get()) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void init(Configuration configuration, DelegationTokenManagerCallback callback) {
        this.callback = callback;
        init(configuration);
    }

    @Override
    public boolean delegationTokensRequired() {
        if (throwInUsage.get()) {
            throw new IllegalArgumentException();
        }
        return addToken.get();
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() {
        if (throwInUsage.get()) {
            throw new IllegalArgumentException();
        }
        if (addToken.get()) {
            return new ObtainedDelegationTokens("TEST_TOKEN_VALUE".getBytes(), Optional.empty());
        } else {
            return null;
        }
    }

    @Override
    public void registerJob(JobID jobId, Configuration jobConfiguration) {
        if (throwInRegister.get()) {
            throw new IllegalArgumentException();
        }
        registeredJobs.get().add(jobId);
        if (throwErrorInRegister.get()) {
            throw new NoClassDefFoundError("simulated classpath failure in provider registerJob");
        }
        if (shouldReobtainOnRegister.get()) {
            callback.reobtainDelegationTokens();
        }
    }

    @Override
    public void unregisterJob(JobID jobId) {
        if (throwInUnregister.get()) {
            throw new IllegalArgumentException();
        }
        if (throwErrorInUnregister.get()) {
            throw new NoClassDefFoundError("simulated classpath failure in provider unregisterJob");
        }
        registeredJobs.get().remove(jobId);
    }

    @Override
    public void stop() {
        stopped.set(true);
    }
}

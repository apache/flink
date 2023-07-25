/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nonnull;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Configuration for the {@link PekkoRpcService}. */
public class PekkoRpcServiceConfiguration {

    @Nonnull private final Configuration configuration;

    @Nonnull private final Duration timeout;

    private final long maximumFramesize;

    private final boolean captureAskCallStack;

    private final boolean forceRpcInvocationSerialization;

    private PekkoRpcServiceConfiguration(
            @Nonnull Configuration configuration,
            @Nonnull Duration timeout,
            long maximumFramesize,
            boolean captureAskCallStack,
            boolean forceRpcInvocationSerialization) {

        checkArgument(maximumFramesize > 0L, "Maximum framesize must be positive.");
        this.configuration = configuration;
        this.timeout = timeout;
        this.maximumFramesize = maximumFramesize;
        this.captureAskCallStack = captureAskCallStack;
        this.forceRpcInvocationSerialization = forceRpcInvocationSerialization;
    }

    @Nonnull
    public Configuration getConfiguration() {
        return configuration;
    }

    @Nonnull
    public Duration getTimeout() {
        return timeout;
    }

    public long getMaximumFramesize() {
        return maximumFramesize;
    }

    public boolean captureAskCallStack() {
        return captureAskCallStack;
    }

    public boolean isForceRpcInvocationSerialization() {
        return forceRpcInvocationSerialization;
    }

    public static PekkoRpcServiceConfiguration fromConfiguration(Configuration configuration) {
        final Duration timeout = configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION);

        final long maximumFramesize = PekkoRpcServiceUtils.extractMaximumFramesize(configuration);

        final boolean captureAskCallStacks = configuration.get(AkkaOptions.CAPTURE_ASK_CALLSTACK);

        final boolean forceRpcInvocationSerialization =
                AkkaOptions.isForceRpcInvocationSerializationEnabled(configuration);

        return new PekkoRpcServiceConfiguration(
                configuration,
                timeout,
                maximumFramesize,
                captureAskCallStacks,
                forceRpcInvocationSerialization);
    }

    public static PekkoRpcServiceConfiguration defaultConfiguration() {
        return fromConfiguration(new Configuration());
    }
}

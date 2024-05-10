/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.util.Disposable;

import javax.annotation.Nonnull;

import java.io.Closeable;

/**
 * An async keyed state backend provides methods supporting to access keyed state asynchronously and
 * in batch.
 */
@Internal
public interface AsyncKeyedStateBackend extends Disposable, Closeable {

    /**
     * Initializes with some contexts.
     *
     * @param stateRequestHandler which handles state request.
     */
    void setup(@Nonnull StateRequestHandler stateRequestHandler);

    /**
     * Creates and returns a new state.
     *
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param <SV> The type of the stored state value.
     * @param <S> The type of the public API state.
     * @throws Exception Exceptions may occur during initialization of the state.
     */
    @Nonnull
    <SV, S extends State> S createState(@Nonnull StateDescriptor<SV> stateDesc) throws Exception;

    /**
     * Creates a {@code StateExecutor} which supports to execute a batch of state requests
     * asynchronously.
     *
     * <p>Notice that the {@code AsyncKeyedStateBackend} is responsible for shutting down the
     * StateExecutors created by itself when they are no longer in use.
     *
     * @return a {@code StateExecutor} which supports to execute a batch of state requests
     *     asynchronously.
     */
    @Nonnull
    StateExecutor createStateExecutor();

    @Override
    void dispose();
}

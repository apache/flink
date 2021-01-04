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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;

/** This factory produces concrete internal state objects. */
public interface KeyedStateFactory {

    /**
     * Creates and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    @Nonnull
    default <N, SV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc)
            throws Exception {
        return createInternalState(
                namespaceSerializer, stateDesc, StateSnapshotTransformFactory.noTransform());
    }

    /**
     * Creates and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param snapshotTransformFactory factory of state snapshot transformer.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <SEV> The type of the stored state value or entry for collection types (list or map).
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    @Nonnull
    <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception;
}

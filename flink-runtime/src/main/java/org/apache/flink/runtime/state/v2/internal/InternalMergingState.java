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

package org.apache.flink.runtime.state.v2.internal;

import org.apache.flink.api.common.state.v2.MergingState;
import org.apache.flink.api.common.state.v2.StateFuture;

import java.util.Collection;

/**
 * This class defines the internal interface for merging state.
 *
 * @param <K> The type of key the state is associated to.
 * @param <N> The namespace type.
 * @param <IN> The type of the values that are added into the state.
 * @param <SV> The type of the intermediate state.
 * @param <OUT> The type of the values that are returned from the state.
 * @param <SYNCOUT> Type of the value that can be retrieved from the state by synchronous interface.
 */
public interface InternalMergingState<K, N, IN, SV, OUT, SYNCOUT>
        extends InternalAppendingState<K, N, IN, SV, OUT, SYNCOUT>, MergingState<IN, OUT, SYNCOUT> {

    /**
     * Merges the state of the current key for the given source namespaces into the state of the
     * target namespace.
     *
     * @param target The target namespace where the merged state should be stored.
     * @param sources The source namespaces whose state should be merged.
     */
    StateFuture<Void> asyncMergeNamespaces(N target, Collection<N> sources);

    /**
     * Merges the state of the current key for the given source namespaces into the state of the
     * target namespace.
     *
     * @param target The target namespace where the merged state should be stored.
     * @param sources The source namespaces whose state should be merged.
     */
    void mergeNamespaces(N target, Collection<N> sources);
}

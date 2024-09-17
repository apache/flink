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

package org.apache.flink.runtime.state.v2.adaptor;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.internal.InternalMergingState;

import java.util.Collection;

/**
 * An adaptor that transforms {@link org.apache.flink.runtime.state.internal.InternalMergingState}
 * into {@link org.apache.flink.runtime.state.v2.internal.InternalMergingState}.
 */
public abstract class MergingStateAdaptor<K, N, IN, ACC, OUT, SYNCOUT>
        extends StateAdaptor<
                K,
                N,
                org.apache.flink.runtime.state.internal.InternalMergingState<
                        K, N, IN, ACC, SYNCOUT>>
        implements InternalMergingState<K, N, IN, ACC, OUT, SYNCOUT> {

    public MergingStateAdaptor(
            org.apache.flink.runtime.state.internal.InternalMergingState<K, N, IN, ACC, SYNCOUT>
                    mergingState) {
        super(mergingState);
    }

    @Override
    public StateFuture<Void> asyncMergeNamespaces(N target, Collection<N> sources) {
        try {
            delegatedState.mergeNamespaces(target, sources);
        } catch (Exception e) {
            throw new RuntimeException("Error while merging namespaces within MergingState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        try {
            delegatedState.mergeNamespaces(target, sources);
        } catch (Exception e) {
            throw new RuntimeException("Error while merging namespaces within MergingState", e);
        }
    }

    @Override
    public SYNCOUT get() {
        try {
            return delegatedState.get();
        } catch (Exception e) {
            throw new RuntimeException("Error while get value from raw MergingState", e);
        }
    }

    @Override
    public StateFuture<Void> asyncAdd(IN value) {
        try {
            delegatedState.add(value);
        } catch (Exception e) {
            throw new RuntimeException("Error while add value to raw MergingState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void add(IN value) {
        try {
            delegatedState.add(value);
        } catch (Exception e) {
            throw new RuntimeException("Error while add value to raw MergingState", e);
        }
    }
}

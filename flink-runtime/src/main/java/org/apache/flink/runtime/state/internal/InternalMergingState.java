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

package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.MergingState;

import java.util.Collection;

/**
 * The peer to the {@link MergingState} in the internal state type hierarchy.
 * 
 * See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> The type of elements added to the state
 * @param <SV> The type of elements in the state
 * @param <OUT> The type of elements
 */
public interface InternalMergingState<K, N, IN, SV, OUT> extends InternalAppendingState<K, N, IN, SV, OUT>, MergingState<IN, OUT> {

	/**
	 * Merges the state of the current key for the given source namespaces into the state of
	 * the target namespace.
	 * 
	 * @param target The target namespace where the merged state should be stored.
	 * @param sources The source namespaces whose state should be merged.
	 * 
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	void mergeNamespaces(N target, Collection<N> sources) throws Exception;
}

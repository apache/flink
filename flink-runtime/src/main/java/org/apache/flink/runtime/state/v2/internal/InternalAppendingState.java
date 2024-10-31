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

import org.apache.flink.api.common.state.v2.AppendingState;
import org.apache.flink.api.common.state.v2.StateFuture;

/**
 * This class defines the internal interface for appending state.
 *
 * @param <K> The type of key the state is associated to.
 * @param <N> The namespace type.
 * @param <IN> The type of the values that are added into the state.
 * @param <SV> The type of the intermediate state.
 * @param <OUT> The type of the values that are returned from the state.
 * @param <SYNCOUT> Type of the value that can be retrieved from the state by synchronous interface.
 */
public interface InternalAppendingState<K, N, IN, SV, OUT, SYNCOUT>
        extends InternalKeyedState<K, N, SV>, AppendingState<IN, OUT, SYNCOUT> {
    /**
     * Get internally stored value.
     *
     * @return internally stored value.
     */
    StateFuture<SV> asyncGetInternal();

    /**
     * Update internally stored value.
     *
     * @param valueToStore new value to store.
     */
    StateFuture<Void> asyncUpdateInternal(SV valueToStore);

    /**
     * Get internally stored value.
     *
     * @return internally stored value.
     */
    SV getInternal();

    /**
     * Update internally stored value.
     *
     * @param valueToStore new value to store.
     */
    void updateInternal(SV valueToStore);
}

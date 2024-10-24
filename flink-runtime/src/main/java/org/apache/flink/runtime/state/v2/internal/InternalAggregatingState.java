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

import org.apache.flink.api.common.state.v2.AggregatingState;

/**
 * This class defines the internal interface for aggregating state.
 *
 * @param <K> The type of key the state is associated to.
 * @param <N> The namespace type.
 * @param <IN> The type of the values that are added into the state.
 * @param <ACC> The type of the accumulator (intermediate aggregation state).
 * @param <OUT> The type of the values that are returned from the state.
 */
public interface InternalAggregatingState<K, N, IN, ACC, OUT>
        extends InternalMergingState<K, N, IN, ACC, OUT, OUT>,
                AggregatingState<IN, OUT>,
                InternalKeyedState<K, N, ACC> {}

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

package org.apache.flink.state.api.input.operator.window;

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.State;

import java.io.Serializable;
import java.util.Collections;

/**
 * An abstraction for transforming any {@link State} type into an iterable over its contents.
 *
 * @param <S> The initial state type.
 * @param <IN> The data in state.
 */
@FunctionalInterface
public interface WindowContents<S extends State, IN> extends Serializable {

    static <T> WindowContents<ReducingState<T>, T> reducingState() {
        return (state) -> Collections.singletonList(state.get());
    }

    static <IN, OUT> WindowContents<AggregatingState<IN, OUT>, OUT> aggregatingState() {
        return (state) -> Collections.singletonList(state.get());
    }

    static <T> WindowContents<ListState<T>, T> listState() {
        return AppendingState::get;
    }

    Iterable<IN> contents(S state) throws Exception;
}

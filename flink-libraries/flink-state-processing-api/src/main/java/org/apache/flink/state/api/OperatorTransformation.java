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

package org.apache.flink.state.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * This class provides the entry point for building {@link StateBootstrapTransformation}s, which
 * represents procedures to bootstrap new operator states with a given {@link DataStream}.
 *
 * <h2>Example usage</h2>
 *
 * <pre>{@code
 * DataStream<StateData> stateData = ...;
 *
 * // to bootstrap non-keyed state:
 * StateBootstrapTransformation<StateData> nonKeyedStateBootstrap = OperatorTransformation
 *     .bootstrapWith(stateData)
 *     .transform(new StateBootstrapFunction<StateData>() {...})
 *
 * // to bootstrap keyed state:
 * StateBootstrapTransformation<StateData> keyedStateBootstrap = OperatorTransformation
 *     .bootstrapWith(stateData)
 *     .keyBy(new KeySelector<StateData, KeyType>() {...})
 *     .transform(new KeyedStateBootstrapFunction<KeyType, StateData>() {...})
 * }</pre>
 *
 * <p>The code example above demonstrates how to create {@code BootstrapTransformation}s for
 * non-keyed and keyed state. The built bootstrap transformations can then used with a {@link
 * SavepointWriter}.
 *
 * @see OneInputStateTransformation
 * @see KeyedStateTransformation
 * @see StateBootstrapTransformation
 */
@PublicEvolving
@SuppressWarnings("WeakerAccess")
public final class OperatorTransformation {

    private OperatorTransformation() {}

    /**
     * Create a new {@link OperatorTransformation} from a {@link DataSet}.
     *
     * @param dataSet A dataset of elements.
     * @param <T> The type of the input.
     * @return A {@link OneInputOperatorTransformation}.
     * @deprecated use {@link #bootstrapWith(DataStream)} to bootstrap a savepoint using the data
     *     stream api under batch execution.
     */
    @Deprecated
    public static <T> OneInputOperatorTransformation<T> bootstrapWith(DataSet<T> dataSet) {
        return new OneInputOperatorTransformation<>(dataSet);
    }

    /**
     * Create a new {@link OneInputStateTransformation} from a {@link DataStream}.
     *
     * @param stream A data stream of elements.
     * @param <T> The type of the input.
     * @return A {@link OneInputStateTransformation}.
     */
    public static <T> OneInputStateTransformation<T> bootstrapWith(DataStream<T> stream) {
        return new OneInputStateTransformation<>(stream);
    }
}

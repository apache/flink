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

package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * The input format for reading {@link org.apache.flink.api.common.state.BroadcastState}.
 *
 * @param <K> The type of the keys in the {@code BroadcastState}.
 * @param <V> The type of the values in the {@code BroadcastState}.
 */
@Internal
public class BroadcastStateInputFormat<K, V> extends OperatorStateInputFormat<Tuple2<K, V>> {

    private static final long serialVersionUID = -7625225340801402409L;

    private final MapStateDescriptor<K, V> descriptor;

    /**
     * Creates an input format for reading broadcast state from an operator in a savepoint.
     *
     * @param operatorState The state to be queried.
     * @param descriptor The descriptor for this state, providing a name and serializer.
     */
    public BroadcastStateInputFormat(
            OperatorState operatorState, MapStateDescriptor<K, V> descriptor) {
        super(operatorState, true);

        this.descriptor =
                Preconditions.checkNotNull(descriptor, "The state descriptor must not be null");
    }

    @Override
    protected final Iterable<Tuple2<K, V>> getElements(OperatorStateBackend restoredBackend)
            throws Exception {
        Iterable<Map.Entry<K, V>> entries = restoredBackend.getBroadcastState(descriptor).entries();

        return () ->
                StreamSupport.stream(entries.spliterator(), false)
                        .map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
                        .iterator();
    }
}

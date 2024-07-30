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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.BroadcastStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MockGlobalDecuplicateCountProcessFunction
        implements OneInputStreamProcessFunction<Integer, Integer> {

    private final BroadcastStateDeclaration<Integer, Integer> broadcastStateDeclaration =
            StateDeclarations.mapStateBuilder(
                            "broadcast-state", TypeDescriptors.INT, TypeDescriptors.INT)
                    .buildBroadcast();

    @Override
    public Set<StateDeclaration> usesStates() {
        return new HashSet<>(Collections.singletonList(broadcastStateDeclaration));
    }

    @Override
    public void processRecord(Integer record, Collector<Integer> output, PartitionedContext ctx)
            throws Exception {
        Optional<BroadcastState<Integer, Integer>> stateOptional =
                ctx.getStateManager().getState(broadcastStateDeclaration);
        if (!stateOptional.isPresent()) {
            throw new RuntimeException("State is not available");
        }
        BroadcastState<Integer, Integer> state = stateOptional.get();
        state.put(record, record);

        int len = 0;
        for (Map.Entry<Integer, Integer> entry : state.entries()) {
            len++;
        }

        output.collect(len);
    }
}

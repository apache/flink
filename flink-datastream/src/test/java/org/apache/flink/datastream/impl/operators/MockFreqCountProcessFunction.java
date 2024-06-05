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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class MockFreqCountProcessFunction
        implements OneInputStreamProcessFunction<Integer, Integer> {

    private final MapStateDeclaration<Integer, Integer> mapStateDeclaration =
            StateDeclarations.mapState("map-state", TypeDescriptors.INT, TypeDescriptors.INT);

    @Override
    public Set<StateDeclaration> usesStates() {
        return new HashSet<>(Collections.singletonList(mapStateDeclaration));
    }

    @Override
    public void processRecord(Integer record, Collector<Integer> output, PartitionedContext ctx)
            throws Exception {
        Optional<MapState<Integer, Integer>> stateOptional =
                ctx.getStateManager().getState(mapStateDeclaration);
        if (!stateOptional.isPresent()) {
            throw new RuntimeException("State is not available");
        }
        MapState<Integer, Integer> state = stateOptional.get();
        Integer oldFreq = state.get(record);
        Integer newFreq = oldFreq == null ? 1 : oldFreq + 1;
        state.put(record, newFreq);

        output.collect(newFreq);
    }
}

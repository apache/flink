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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDeclaration;
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

public class MockRecudingMultiplierProcessFunction
        implements OneInputStreamProcessFunction<Integer, Integer> {

    private final ReducingStateDeclaration<Integer> reducingStateDeclaration =
            StateDeclarations.reducingState(
                    "reducing-state",
                    TypeDescriptors.INT,
                    new ReduceFunction<Integer>() {
                        @Override
                        public Integer reduce(Integer value1, Integer value2) throws Exception {
                            return value2 * value1;
                        }
                    });

    @Override
    public Set<StateDeclaration> usesStates() {
        return new HashSet<>(Collections.singletonList(reducingStateDeclaration));
    }

    @Override
    public void processRecord(Integer record, Collector<Integer> output, PartitionedContext ctx)
            throws Exception {
        Optional<ReducingState<Integer>> stateOptional =
                ctx.getStateManager().getState(reducingStateDeclaration);
        if (!stateOptional.isPresent()) {
            throw new RuntimeException("State is not available");
        }
        ReducingState<Integer> state = stateOptional.get();
        state.add(record);

        output.collect(state.get());
    }
}

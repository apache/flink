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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.junit.jupiter.api.Test;

/** Tests for {@link InternalReducingState}. */
public class InternalReducingStateTest extends InternalKeyedStateTestBase {

    @Test
    @SuppressWarnings({"unchecked"})
    public void testEachOperation() {
        ReduceFunction<Integer> reducer = Integer::sum;
        ReducingStateDescriptor<Integer> descriptor =
                new ReducingStateDescriptor<>("testState", reducer, BasicTypeInfo.INT_TYPE_INFO);
        InternalReducingState<String, Integer> reducingState =
                new InternalReducingState<>(aec, descriptor);
        aec.setCurrentContext(aec.buildContext("test", "test"));

        reducingState.asyncClear();
        validateRequestRun(reducingState, StateRequestType.CLEAR, null);

        reducingState.asyncGet();
        validateRequestRun(reducingState, StateRequestType.REDUCING_GET, null);

        reducingState.asyncAdd(1);
        validateRequestRun(reducingState, StateRequestType.REDUCING_ADD, 1);
    }
}

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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.junit.jupiter.api.Test;

/** Tests for {@link AbstractValueState}. */
public class AbstractValueStateTest extends AbstractKeyedStateTestBase {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testEachOperation() {
        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("testState", BasicTypeInfo.INT_TYPE_INFO);
        descriptor.initializeSerializerUnlessSet(new ExecutionConfig());
        AbstractValueState<String, Void, Integer> valueState =
                new AbstractValueState<>(aec, descriptor.getSerializer());
        aec.setCurrentContext(aec.buildContext("test", "test"));

        valueState.asyncClear();
        validateRequestRun(valueState, StateRequestType.CLEAR, null, 0);

        valueState.asyncValue();
        validateRequestRun(valueState, StateRequestType.VALUE_GET, null, 0);

        valueState.asyncUpdate(1);
        validateRequestRun(valueState, StateRequestType.VALUE_UPDATE, 1, 0);

        valueState.clear();
        validateRequestRun(valueState, StateRequestType.CLEAR, null, 0);

        valueState.value();
        validateRequestRun(valueState, StateRequestType.VALUE_GET, null, 0);

        valueState.update(1);
        validateRequestRun(valueState, StateRequestType.VALUE_UPDATE, 1, 0);
    }
}

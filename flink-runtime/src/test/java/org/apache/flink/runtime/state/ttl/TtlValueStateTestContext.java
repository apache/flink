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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

/** Test suite for {@link TtlValueState}. */
class TtlValueStateTestContext
        extends TtlStateTestContextBase<TtlValueState<?, String, String>, String, String> {
    private static final String TEST_VAL1 = "test value1";
    private static final String TEST_VAL2 = "test value2";
    private static final String TEST_VAL3 = "test value3";

    @Override
    void initTestValues() {
        updateEmpty = TEST_VAL1;
        updateUnexpired = TEST_VAL2;
        updateExpired = TEST_VAL3;

        getUpdateEmpty = TEST_VAL1;
        getUnexpired = TEST_VAL2;
        getUpdateExpired = TEST_VAL3;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <US extends State, SV> StateDescriptor<US, SV> createStateDescriptor() {
        return (StateDescriptor<US, SV>)
                new ValueStateDescriptor<>(getName(), StringSerializer.INSTANCE);
    }

    @Override
    public void update(String value) throws Exception {
        ttlState.update(value);
    }

    @Override
    public String get() throws Exception {
        return ttlState.value();
    }

    @Override
    public Object getOriginal() throws Exception {
        return ttlState.original.value();
    }
}

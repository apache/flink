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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link InternalListState}. */
public class InternalListStateTest extends InternalKeyedStateTestBase {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testEachOperation() {
        ListStateDescriptor<Integer> descriptor =
                new ListStateDescriptor<>("testState", BasicTypeInfo.INT_TYPE_INFO);
        InternalListState<String, Integer> listState = new InternalListState<>(aec, descriptor);
        aec.setCurrentContext(aec.buildContext("test", "test"));

        listState.asyncClear();
        validateRequestRun(listState, StateRequestType.CLEAR, null);

        listState.asyncGet();
        validateRequestRun(listState, StateRequestType.LIST_GET, null);

        listState.asyncAdd(1);
        validateRequestRun(listState, StateRequestType.LIST_ADD, 1);

        List<Integer> list = new ArrayList<>();
        listState.asyncUpdate(list);
        validateRequestRun(listState, StateRequestType.LIST_UPDATE, list);

        list = new ArrayList<>();
        listState.asyncAddAll(list);
        validateRequestRun(listState, StateRequestType.LIST_ADD_ALL, list);
    }
}

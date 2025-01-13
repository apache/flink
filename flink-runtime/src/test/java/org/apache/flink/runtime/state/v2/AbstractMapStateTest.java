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

import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Tests for {@link AbstractMapState}. */
public class AbstractMapStateTest extends AbstractKeyedStateTestBase {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testEachOperation() {
        MapStateDescriptor<String, Integer> descriptor =
                new MapStateDescriptor<>(
                        "testState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
        AbstractMapState<String, Void, String, Integer> mapState =
                new AbstractMapState<>(aec, descriptor);
        aec.setCurrentContext(aec.buildContext("test", "test"));

        mapState.asyncClear();
        validateRequestRun(mapState, StateRequestType.CLEAR, null, 0);

        mapState.asyncGet("key1");
        validateRequestRun(mapState, StateRequestType.MAP_GET, "key1", 0);

        mapState.asyncPut("key2", 2);
        validateRequestRun(mapState, StateRequestType.MAP_PUT, Tuple2.of("key2", 2), 0);

        Map<String, Integer> map = new HashMap<>();
        mapState.asyncPutAll(map);
        validateRequestRun(mapState, StateRequestType.MAP_PUT_ALL, map, 0);

        mapState.asyncRemove("key3");
        validateRequestRun(mapState, StateRequestType.MAP_REMOVE, "key3", 0);

        mapState.asyncContains("key4");
        validateRequestRun(mapState, StateRequestType.MAP_CONTAINS, "key4", 0);

        mapState.asyncEntries();
        validateRequestRun(mapState, StateRequestType.MAP_ITER, null, 0);

        mapState.asyncKeys();
        validateRequestRun(mapState, StateRequestType.MAP_ITER_KEY, null, 0);

        mapState.asyncValues();
        validateRequestRun(mapState, StateRequestType.MAP_ITER_VALUE, null, 0);

        mapState.asyncIsEmpty();
        validateRequestRun(mapState, StateRequestType.MAP_IS_EMPTY, null, 0);

        mapState.clear();
        validateRequestRun(mapState, StateRequestType.CLEAR, null, 0);

        mapState.get("key1");
        validateRequestRun(mapState, StateRequestType.MAP_GET, "key1", 0);

        mapState.put("key2", 2);
        validateRequestRun(mapState, StateRequestType.MAP_PUT, Tuple2.of("key2", 2), 0);

        mapState.putAll(map);
        validateRequestRun(mapState, StateRequestType.MAP_PUT_ALL, map, 0);

        mapState.remove("key3");
        validateRequestRun(mapState, StateRequestType.MAP_REMOVE, "key3", 0);

        mapState.contains("key4");
        validateRequestRun(mapState, StateRequestType.MAP_CONTAINS, "key4", 0);

        mapState.iterator();
        validateRequestRun(mapState, StateRequestType.MAP_ITER, null, 0);

        mapState.entries().iterator();
        validateRequestRun(mapState, StateRequestType.MAP_ITER, null, 0);

        mapState.keys().iterator();
        validateRequestRun(mapState, StateRequestType.MAP_ITER_KEY, null, 0);

        mapState.values().iterator();
        validateRequestRun(mapState, StateRequestType.MAP_ITER_VALUE, null, 0);

        mapState.isEmpty();
        validateRequestRun(mapState, StateRequestType.MAP_IS_EMPTY, null, 0);
    }
}

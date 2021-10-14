/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog.restore;

import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.state.changelog.StateChangeOperation;

class MapStateChangeApplier<K, N, UK, UV> extends KvStateChangeApplier<K, N> {
    private final InternalMapState<K, N, UK, UV> mapState;
    private final MapSerializer<UK, UV> mapSerializer;

    protected MapStateChangeApplier(
            InternalMapState<K, N, UK, UV> mapState, InternalKeyContext<K> keyContext) {
        super(keyContext);
        this.mapState = mapState;
        this.mapSerializer = (MapSerializer<UK, UV>) mapState.getValueSerializer();
    }

    @Override
    protected InternalKvState<K, N, ?> getState() {
        return mapState;
    }

    protected void applyInternal(StateChangeOperation operation, DataInputView in)
            throws Exception {
        switch (operation) {
            case ADD:
                mapState.putAll(mapSerializer.deserialize(in));
                break;
            case ADD_ELEMENT:
            case ADD_OR_UPDATE_ELEMENT:
                mapState.put(
                        mapSerializer.getKeySerializer().deserialize(in),
                        mapSerializer.getValueSerializer().deserialize(in));
                break;
            case REMOVE_ELEMENT:
                mapState.remove(mapSerializer.getKeySerializer().deserialize(in));
                break;
            case CLEAR:
                mapState.clear();
                break;
            default:
                throw new IllegalArgumentException("Unknown state change operation: " + operation);
        }
    }
}

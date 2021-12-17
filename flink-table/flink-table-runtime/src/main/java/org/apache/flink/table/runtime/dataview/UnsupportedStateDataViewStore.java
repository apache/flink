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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * A {@link StateDataViewStore} to throw {@link UnsupportedOperationException} when creating {@link
 * StateDataView}.
 */
public final class UnsupportedStateDataViewStore implements StateDataViewStore {

    private final RuntimeContext runtimeContext;

    public UnsupportedStateDataViewStore(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public <N, EK, EV> StateMapView<N, EK, EV> getStateMapView(
            String stateName,
            boolean supportNullKey,
            TypeSerializer<EK> keySerializer,
            TypeSerializer<EV> valueSerializer)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public <N, EE> StateListView<N, EE> getStateListView(
            String stateName, TypeSerializer<EE> elementSerializer) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }
}

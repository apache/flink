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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** This interface contains methods for registering {@link StateDataView} with a managed store. */
@Internal
public interface StateDataViewStore {

    /**
     * Creates a state map view.
     *
     * @param stateName The name of underlying state of the map view
     * @param supportNullKey Whether the null key should be supported
     * @param keySerializer The key serializer
     * @param valueSerializer The value serializer
     * @param <N> Type of the namespace
     * @param <EK> External type of the keys in the map state
     * @param <EV> External type of the values in the map state
     * @return a keyed map state
     */
    <N, EK, EV> StateMapView<N, EK, EV> getStateMapView(
            String stateName,
            boolean supportNullKey,
            TypeSerializer<EK> keySerializer,
            TypeSerializer<EV> valueSerializer)
            throws Exception;

    /**
     * Creates a state list view.
     *
     * @param stateName The name of underlying state of the list view
     * @param elementSerializer The element serializer
     * @param <N> Type of the namespace
     * @param <EE> External type of the elements in the list state
     * @return a keyed list state
     */
    <N, EE> StateListView<N, EE> getStateListView(
            String stateName, TypeSerializer<EE> elementSerializer) throws Exception;

    RuntimeContext getRuntimeContext();
}

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

package org.apache.flink.state.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nullable;

/**
 * Mixed into mapping classes that back a table with exactly one flattened LIST/MAP state, described
 * by a single fixed descriptor rather than a list of value columns. Implemented by {@link
 * FlattenedStateTableMapping} and {@link WindowFlattenedStateTableMapping}, allowing {@link
 * AbstractSingleColumnScanProvider} to build their state descriptor generically.
 */
@Internal
@SuppressWarnings("rawtypes")
interface SingleColumnStateMapping extends SavepointStateMapping {

    String getStateName();

    SavepointConnectorOptions.StateType getStateType();

    @Nullable
    TypeSerializer getMapKeyTypeSerializer();

    TypeSerializer getValueTypeSerializer();

    void setStateDescriptor(StateDescriptor stateDescriptor);
}

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

import org.apache.flink.api.common.state.StateDescriptor;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Configuration for SQL state columns. */
@SuppressWarnings("rawtypes")
public class StateValueColumnConfiguration implements Serializable {
    private final int columnIndex;
    private final String stateName;
    private final SavepointConnectorOptions.StateType stateType;
    @Nullable private final String mapKeyFormat;
    private final String valueFormat;
    @Nullable private StateDescriptor stateDescriptor;

    public StateValueColumnConfiguration(
            int columnIndex,
            final String stateName,
            final SavepointConnectorOptions.StateType stateType,
            @Nullable final String mapKeyFormat,
            @Nullable final String valueFormat) {
        this.columnIndex = columnIndex;
        this.stateName = stateName;
        this.stateType = stateType;
        this.mapKeyFormat = mapKeyFormat;
        this.valueFormat = valueFormat;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getStateName() {
        return stateName;
    }

    public SavepointConnectorOptions.StateType getStateType() {
        return stateType;
    }

    @Nullable
    public String getMapKeyFormat() {
        return mapKeyFormat;
    }

    public String getValueFormat() {
        return valueFormat;
    }

    public void setStateDescriptor(StateDescriptor stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Nullable
    public StateDescriptor getStateDescriptor() {
        return stateDescriptor;
    }
}

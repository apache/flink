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

/**
 * Utilities for transforming {@link StateDescriptor} to {@link
 * org.apache.flink.api.common.state.StateDescriptor}.
 */
public class StateDescriptorUtils {
    private StateDescriptorUtils() {}

    public static org.apache.flink.api.common.state.StateDescriptor transformFromV2ToV1(
            StateDescriptor stateDescriptor) {
        switch (stateDescriptor.getType()) {
            case VALUE:
                ValueStateDescriptor valueStateDesc = (ValueStateDescriptor) stateDescriptor;
                return new org.apache.flink.api.common.state.ValueStateDescriptor(
                        valueStateDesc.getStateId(), valueStateDesc.getSerializer());

            case MAP:
                MapStateDescriptor mapStateDesc = (MapStateDescriptor) stateDescriptor;
                return new org.apache.flink.api.common.state.MapStateDescriptor<>(
                        mapStateDesc.getStateId(),
                        mapStateDesc.getUserKeySerializer(),
                        mapStateDesc.getSerializer());

            case LIST:
                ListStateDescriptor listStateDesc = (ListStateDescriptor) stateDescriptor;
                return new org.apache.flink.api.common.state.ListStateDescriptor<>(
                        listStateDesc.getStateId(), listStateDesc.getSerializer());

            case REDUCING:
                ReducingStateDescriptor reducingStateDesc =
                        (ReducingStateDescriptor) stateDescriptor;
                return new org.apache.flink.api.common.state.ReducingStateDescriptor(
                        reducingStateDesc.getStateId(),
                        reducingStateDesc.getReduceFunction(),
                        reducingStateDesc.getSerializer());

            case AGGREGATING:
                AggregatingStateDescriptor aggregatingStateDesc =
                        (AggregatingStateDescriptor) stateDescriptor;
                return new org.apache.flink.api.common.state.AggregatingStateDescriptor(
                        aggregatingStateDesc.getStateId(),
                        aggregatingStateDesc.getAggregateFunction(),
                        aggregatingStateDesc.getSerializer());
            default:
                throw new IllegalArgumentException(
                        "Unsupported state type: " + stateDescriptor.getType());
        }
    }
}

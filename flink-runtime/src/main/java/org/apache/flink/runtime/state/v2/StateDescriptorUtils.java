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

import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;

/**
 * Utilities for transforming {@link StateDescriptor} to {@link
 * org.apache.flink.api.common.state.StateDescriptor}.
 */
public class StateDescriptorUtils {
    private StateDescriptorUtils() {}

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static org.apache.flink.api.common.state.StateDescriptor transformFromV2ToV1(
            StateDescriptor stateDescriptor) {
        switch (stateDescriptor.getType()) {
            case VALUE:
                return transformFromV2ToV1((ValueStateDescriptor) stateDescriptor);
            case MAP:
                return transformFromV2ToV1((MapStateDescriptor) stateDescriptor);
            case LIST:
                return transformFromV2ToV1((ListStateDescriptor) stateDescriptor);
            case REDUCING:
                return transformFromV2ToV1((ReducingStateDescriptor) stateDescriptor);
            case AGGREGATING:
                return transformFromV2ToV1((AggregatingStateDescriptor) stateDescriptor);
            default:
                throw new IllegalArgumentException(
                        "Unsupported state type: " + stateDescriptor.getType());
        }
    }

    public static <V> org.apache.flink.api.common.state.ValueStateDescriptor<V> transformFromV2ToV1(
            ValueStateDescriptor<V> stateDescriptor) {
        if (stateDescriptor.isSerializerInitialized()) {
            return new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    stateDescriptor.getStateId(), stateDescriptor.getSerializer());
        } else {
            return new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    stateDescriptor.getStateId(), stateDescriptor.getTypeInformation());
        }
    }

    public static <UK, UV>
            org.apache.flink.api.common.state.MapStateDescriptor<UK, UV> transformFromV2ToV1(
                    MapStateDescriptor<UK, UV> stateDescriptor) {
        if (stateDescriptor.isSerializerInitialized()) {
            return new org.apache.flink.api.common.state.MapStateDescriptor<>(
                    stateDescriptor.getStateId(),
                    stateDescriptor.getUserKeySerializer(),
                    stateDescriptor.getSerializer());
        } else {
            return new org.apache.flink.api.common.state.MapStateDescriptor<>(
                    stateDescriptor.getStateId(),
                    stateDescriptor.getUserKeyTypeInformation(),
                    stateDescriptor.getTypeInformation());
        }
    }

    public static <V> org.apache.flink.api.common.state.ListStateDescriptor<V> transformFromV2ToV1(
            ListStateDescriptor<V> stateDescriptor) {
        if (stateDescriptor.isSerializerInitialized()) {
            return new org.apache.flink.api.common.state.ListStateDescriptor<>(
                    stateDescriptor.getStateId(), stateDescriptor.getSerializer());
        } else {
            return new org.apache.flink.api.common.state.ListStateDescriptor<>(
                    stateDescriptor.getStateId(), stateDescriptor.getTypeInformation());
        }
    }

    public static <V>
            org.apache.flink.api.common.state.ReducingStateDescriptor<V> transformFromV2ToV1(
                    ReducingStateDescriptor<V> stateDescriptor) {
        if (stateDescriptor.isSerializerInitialized()) {
            return new org.apache.flink.api.common.state.ReducingStateDescriptor<>(
                    stateDescriptor.getStateId(),
                    stateDescriptor.getReduceFunction(),
                    stateDescriptor.getSerializer());
        } else {
            return new org.apache.flink.api.common.state.ReducingStateDescriptor<>(
                    stateDescriptor.getStateId(),
                    stateDescriptor.getReduceFunction(),
                    stateDescriptor.getTypeInformation());
        }
    }

    public static <IN, ACC, OUT>
            org.apache.flink.api.common.state.AggregatingStateDescriptor<IN, ACC, OUT>
                    transformFromV2ToV1(AggregatingStateDescriptor<IN, ACC, OUT> stateDescriptor) {
        if (stateDescriptor.isSerializerInitialized()) {
            return new org.apache.flink.api.common.state.AggregatingStateDescriptor<>(
                    stateDescriptor.getStateId(),
                    stateDescriptor.getAggregateFunction(),
                    stateDescriptor.getSerializer());
        } else {
            return new org.apache.flink.api.common.state.AggregatingStateDescriptor<>(
                    stateDescriptor.getStateId(),
                    stateDescriptor.getAggregateFunction(),
                    stateDescriptor.getTypeInformation());
        }
    }
}

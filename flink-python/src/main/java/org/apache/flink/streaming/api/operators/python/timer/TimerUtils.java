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

package org.apache.flink.streaming.api.operators.python.timer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.utils.ProtoUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;

/** Utilities for timer. */
@Internal
public final class TimerUtils {

    public static TypeInformation<Row> createTimerDataTypeInfo(TypeInformation<Row> keyType) {
        // structure: [timerType/timerOperationType, watermark, timestamp, key, namespace]
        // 1) setTimer: [timerOperationType, null, timestamp, key, namespace]
        // 2) onTimer: [timerType, watermark, timestamp, key, namespace]
        return Types.ROW(
                Types.BYTE, Types.LONG, Types.LONG, keyType, Types.PRIMITIVE_ARRAY(Types.BYTE));
    }

    public static FlinkFnApi.CoderInfoDescriptor createTimerDataCoderInfoDescriptorProto(
            TypeInformation<Row> timerDataType) {
        return ProtoUtils.createRawTypeCoderInfoDescriptorProto(
                timerDataType, FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE, false);
    }

    @SuppressWarnings("unchecked")
    public static InternalPriorityQueue<TimerHeapInternalTimer<?, ?>>
            getInternalEventTimeTimersQueue(InternalTimerService<?> internalTimerService)
                    throws Exception {
        Field queueField = internalTimerService.getClass().getDeclaredField("eventTimeTimersQueue");
        queueField.setAccessible(true);
        return (InternalPriorityQueue<TimerHeapInternalTimer<?, ?>>)
                queueField.get(internalTimerService);
    }

    public static boolean hasEventTimeTimerBeforeTimestamp(
            InternalPriorityQueue<TimerHeapInternalTimer<?, ?>> timerQueue,
            long timestamp,
            boolean isBatchMode)
            throws Exception {
        if (isBatchMode) {
            Preconditions.checkArgument(timestamp == Long.MAX_VALUE);
            return timerQueue.size() == 0;
        }

        TimerHeapInternalTimer<?, ?> minTimer = timerQueue.peek();
        return minTimer == null || minTimer.getTimestamp() > timestamp;
    }
}

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

package org.apache.flink.streaming.api.utils.output;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.types.Row;

/** This handler can accepts the runner output which contains timer registration event. */
public class OutputWithTimerRowHandler {

    private final KeyedStateBackend<Row> keyedStateBackend;
    private final TimerService timerService;
    private final TimestampedCollector collector;
    private final KeyContext keyContext;

    public OutputWithTimerRowHandler(
            KeyedStateBackend<Row> keyedStateBackend,
            TimerService timerService,
            TimestampedCollector collector,
            KeyContext keyContext) {
        this.keyedStateBackend = keyedStateBackend;
        this.timerService = timerService;
        this.collector = collector;
        this.keyContext = keyContext;
    }

    public void accept(Row runnerOutput, long timestamp) {
        if (runnerOutput.getField(0) == null) {
            // null represents normal data
            onData(timestamp, runnerOutput.getField(3));
        } else {
            TimerOperandType operandType =
                    TimerOperandType.valueOf((byte) runnerOutput.getField(0));
            onTimerOperation(
                    operandType, (long) runnerOutput.getField(1), (Row) runnerOutput.getField(2));
        }
    }

    private void onTimerOperation(TimerOperandType operandType, long time, Row key) {
        synchronized (keyedStateBackend) {
            keyContext.setCurrentKey(key);
            switch (operandType) {
                case REGISTER_EVENT_TIMER:
                    timerService.registerEventTimeTimer(time);
                    break;
                case REGISTER_PROC_TIMER:
                    timerService.registerProcessingTimeTimer(time);
                    break;
                case DELETE_EVENT_TIMER:
                    timerService.deleteEventTimeTimer(time);
                    break;
                case DELETE_PROC_TIMER:
                    timerService.deleteProcessingTimeTimer(time);
            }
        }
    }

    private void onData(long timestamp, Object data) {
        if (timestamp != Long.MIN_VALUE) {
            collector.setAbsoluteTimestamp(timestamp);
        } else {
            collector.eraseTimestamp();
        }
        collector.collect(data);
    }

    public static TypeInformation<Row> getRunnerOutputTypeInfo(
            TypeInformation<?> outputType, TypeInformation<Row> keyType) {
        // structure: [timerOperandType, timestamp, key, userOutput]
        // for more details about the output flag, see `TimerOperandType`
        return Types.ROW(Types.BYTE, Types.LONG, keyType, outputType);
    }
}

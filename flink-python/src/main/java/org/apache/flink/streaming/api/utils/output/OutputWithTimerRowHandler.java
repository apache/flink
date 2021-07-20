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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.types.Row;

import java.io.IOException;

/** This handler can accepts the runner output which contains timer registration event. */
public class OutputWithTimerRowHandler {

    private final KeyedStateBackend<Row> keyedStateBackend;
    private final InternalTimerService internalTimerService;
    private final TimestampedCollector collector;
    private final KeyContext keyContext;
    private final TypeSerializer namespaceSerializer;
    private final ByteArrayInputStreamWithPos bais;
    private final DataInputViewStreamWrapper baisWrapper;

    public OutputWithTimerRowHandler(
            KeyedStateBackend<Row> keyedStateBackend,
            InternalTimerService internalTimerService,
            TimestampedCollector collector,
            KeyContext keyContext,
            TypeSerializer namespaceSerializer) {
        this.keyedStateBackend = keyedStateBackend;
        this.internalTimerService = internalTimerService;
        this.collector = collector;
        this.keyContext = keyContext;
        this.namespaceSerializer = namespaceSerializer;
        this.bais = new ByteArrayInputStreamWithPos();
        this.baisWrapper = new DataInputViewStreamWrapper(bais);
    }

    public void accept(Row runnerOutput, long timestamp) throws IOException {
        switch (RunnerOutputType.valueOf((byte) runnerOutput.getField(0))) {
            case NORMAL_RECORD:
                onData(timestamp, runnerOutput.getField(1));
                break;
            case TIMER_OPERATION:
                Row timerData = (Row) runnerOutput.getField(2);
                assert timerData != null;

                TimerOperandType operandType =
                        TimerOperandType.valueOf((byte) timerData.getField(0));
                Row key = (Row) timerData.getField(1);
                long time = (long) timerData.getField(2);
                byte[] encodedNamespace = (byte[]) timerData.getField(3);
                assert encodedNamespace != null;

                bais.setBuffer(encodedNamespace, 0, encodedNamespace.length);
                Object namespace;
                if (namespaceSerializer instanceof VoidNamespaceSerializer) {
                    namespace = VoidNamespace.INSTANCE;
                } else {
                    namespace = namespaceSerializer.deserialize(baisWrapper);
                }
                onTimerOperation(operandType, time, key, namespace);
        }
    }

    private void onTimerOperation(
            TimerOperandType operandType, long time, Row key, Object namespace) {
        synchronized (keyedStateBackend) {
            keyContext.setCurrentKey(key);
            switch (operandType) {
                case REGISTER_EVENT_TIMER:
                    internalTimerService.registerEventTimeTimer(namespace, time);
                    break;
                case REGISTER_PROC_TIMER:
                    internalTimerService.registerProcessingTimeTimer(namespace, time);
                    break;
                case DELETE_EVENT_TIMER:
                    internalTimerService.deleteEventTimeTimer(namespace, time);
                    break;
                case DELETE_PROC_TIMER:
                    internalTimerService.deleteProcessingTimeTimer(namespace, time);
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
        // structure: [runnerOutputType, normal_data, timer_data]
        // timer data structure: [timerOperandType, key, timestamp, encoded_namespace]
        return Types.ROW(
                Types.BYTE,
                outputType,
                Types.ROW(Types.BYTE, keyType, Types.LONG, Types.PRIMITIVE_ARRAY(Types.BYTE)));
    }
}

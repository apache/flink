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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/** Handles the interaction with the Python worker for registering and deleting timers. */
@Internal
public final class TimerRegistration {

    private final KeyedStateBackend<Row> keyedStateBackend;
    private final InternalTimerService internalTimerService;
    private final KeyContext keyContext;
    private final TypeSerializer namespaceSerializer;
    private final TypeSerializer<Row> timerDataSerializer;
    private final ByteArrayInputStreamWithPos bais;
    private final DataInputViewStreamWrapper baisWrapper;

    public TimerRegistration(
            KeyedStateBackend<Row> keyedStateBackend,
            InternalTimerService internalTimerService,
            KeyContext keyContext,
            TypeSerializer namespaceSerializer,
            TypeSerializer<Row> timerDataSerializer)
            throws Exception {
        this.keyedStateBackend = keyedStateBackend;
        this.internalTimerService = internalTimerService;
        this.keyContext = keyContext;
        this.namespaceSerializer = namespaceSerializer;
        this.timerDataSerializer = timerDataSerializer;
        this.bais = new ByteArrayInputStreamWithPos();
        this.baisWrapper = new DataInputViewStreamWrapper(bais);
    }

    public void setTimer(byte[] serializedTimerData) {
        try {
            bais.setBuffer(serializedTimerData, 0, serializedTimerData.length);
            Row timerData = timerDataSerializer.deserialize(baisWrapper);
            TimerOperandType operandType = TimerOperandType.valueOf((byte) timerData.getField(0));
            long timestamp = (long) timerData.getField(2);
            Row key = (Row) timerData.getField(3);

            Object namespace;
            if (namespaceSerializer instanceof VoidNamespaceSerializer) {
                namespace = VoidNamespace.INSTANCE;
            } else {
                byte[] encodedNamespace = (byte[]) timerData.getField(4);
                assert encodedNamespace != null;

                bais.setBuffer(encodedNamespace, 0, encodedNamespace.length);
                namespace = namespaceSerializer.deserialize(baisWrapper);
            }
            setTimer(operandType, timestamp, key, namespace);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void setTimer(TimerOperandType operandType, long timestamp, Row key, Object namespace)
            throws Exception {
        synchronized (keyedStateBackend) {
            keyContext.setCurrentKey(key);
            PythonOperatorUtils.setCurrentKeyForTimerService(internalTimerService, key);
            switch (operandType) {
                case REGISTER_EVENT_TIMER:
                    internalTimerService.registerEventTimeTimer(namespace, timestamp);
                    break;
                case REGISTER_PROC_TIMER:
                    internalTimerService.registerProcessingTimeTimer(namespace, timestamp);
                    break;
                case DELETE_EVENT_TIMER:
                    internalTimerService.deleteEventTimeTimer(namespace, timestamp);
                    break;
                case DELETE_PROC_TIMER:
                    internalTimerService.deleteProcessingTimeTimer(namespace, timestamp);
            }
        }
    }

    /** The flag for indicating the timer operation type. */
    private enum TimerOperandType {
        REGISTER_EVENT_TIMER((byte) 0),
        REGISTER_PROC_TIMER((byte) 1),
        DELETE_EVENT_TIMER((byte) 2),
        DELETE_PROC_TIMER((byte) 3);

        private final byte value;

        TimerOperandType(byte value) {
            this.value = value;
        }

        private static final Map<Byte, TimerOperandType> mapping;

        static {
            mapping = new HashMap<>();
            for (TimerOperandType timerOperandType : TimerOperandType.values()) {
                mapping.put(timerOperandType.value, timerOperandType);
            }
        }

        public static TimerOperandType valueOf(byte value) {
            if (mapping.containsKey(value)) {
                return mapping.get(value);
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Value '%d' cannot be converted to TimerOperandType.", value));
            }
        }
    }
}

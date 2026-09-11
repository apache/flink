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

package org.apache.flink.table.runtime.operators.python.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistrationHandler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.process.WritableInternalTimeContext;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.setCurrentKeyForStreaming;

/** Applies process table function timer commands in the operator mailbox thread. */
@Internal
final class ProcessTableTimerRegistration implements TimerRegistrationHandler {

    static final byte REGISTER_ANONYMOUS = 0;
    static final byte REGISTER_NAMED = 1;
    static final byte DELETE_ANONYMOUS = 2;
    static final byte DELETE_NAMED = 3;
    static final byte CLEAR_ALL = 4;
    static final byte TRIGGER = 5;

    private final KeyContext keyContext;
    private final KeyedStateBackend<?> keyedStateBackend;
    private final WritableInternalTimeContext timeContext;
    private final TypeSerializer<RowData> timerDataSerializer;
    private final int keyArity;
    private final ByteArrayInputStreamWithPos inputStream;
    private final DataInputViewStreamWrapper inputView;

    ProcessTableTimerRegistration(
            KeyContext keyContext,
            KeyedStateBackend<?> keyedStateBackend,
            WritableInternalTimeContext timeContext,
            TypeSerializer<RowData> timerDataSerializer,
            int keyArity) {
        this.keyContext = keyContext;
        this.keyedStateBackend = keyedStateBackend;
        this.timeContext = timeContext;
        this.timerDataSerializer = timerDataSerializer;
        this.keyArity = keyArity;
        this.inputStream = new ByteArrayInputStreamWithPos();
        this.inputView = new DataInputViewStreamWrapper(inputStream);
    }

    @Override
    public void setTimer(byte[] serializedTimerData) {
        try {
            inputStream.setBuffer(serializedTimerData, 0, serializedTimerData.length);
            final RowData command = timerDataSerializer.deserialize(inputView);
            final byte operation = command.getByte(0);
            final RowData key = toBackendKey(command.getRow(3, keyArity));
            synchronized (keyedStateBackend) {
                setBackendCurrentKey(key);
                keyContext.setCurrentKey(key);
                switch (operation) {
                    case REGISTER_ANONYMOUS:
                        timeContext.registerOnTime(command.getLong(1));
                        break;
                    case REGISTER_NAMED:
                        timeContext.registerOnTime(
                                command.getString(2).toString(), command.getLong(1));
                        break;
                    case DELETE_ANONYMOUS:
                        timeContext.clearTimer(command.getLong(1));
                        break;
                    case DELETE_NAMED:
                        timeContext.clearTimer(command.getString(2).toString());
                        break;
                    case CLEAR_ALL:
                        timeContext.clearAllTimers();
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unknown PTF timer operation: " + operation);
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to apply a Python PTF timer command.", e);
        }
    }

    private RowData toBackendKey(RowData key) {
        final TypeSerializer<?> serializer = keyedStateBackend.getKeySerializer();
        if (serializer instanceof RowDataSerializer) {
            return ((RowDataSerializer) serializer).toBinaryRow(key).copy();
        }
        return key;
    }

    private void setBackendCurrentKey(RowData key) {
        setCurrentKeyForStreaming((KeyedStateBackend<RowData>) keyedStateBackend, key);
    }
}

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

import java.util.HashMap;
import java.util.Map;

/** The Flag for indicating the timer operation type. */
public enum TimerOperandType {
    REGISTER_EVENT_TIMER((byte) 0),
    REGISTER_PROC_TIMER((byte) 1),
    DELETE_EVENT_TIMER((byte) 2),
    DELETE_PROC_TIMER((byte) 3);

    private final byte value;

    TimerOperandType(byte value) {
        this.value = value;
    }

    private static Map<Byte, TimerOperandType> mapping;

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
                    String.format("Value '%d' can not be converted to TimerOperandType.", value));
        }
    }
}

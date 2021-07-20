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

/** The type of the runner output. */
public enum RunnerOutputType {
    NORMAL_RECORD((byte) 0),
    TIMER_OPERATION((byte) 1);

    private final byte value;

    RunnerOutputType(byte value) {
        this.value = value;
    }

    private static Map<Byte, RunnerOutputType> mapping;

    static {
        mapping = new HashMap<>();
        for (RunnerOutputType runnerOutputType : RunnerOutputType.values()) {
            mapping.put(runnerOutputType.value, runnerOutputType);
        }
    }

    public static RunnerOutputType valueOf(byte value) {
        if (mapping.containsKey(value)) {
            return mapping.get(value);
        } else {
            throw new IllegalArgumentException(
                    String.format("Value '%d' can not be converted to RunnerOutputType.", value));
        }
    }
}

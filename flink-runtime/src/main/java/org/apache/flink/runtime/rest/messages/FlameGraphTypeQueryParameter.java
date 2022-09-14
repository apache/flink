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

package org.apache.flink.runtime.rest.messages;

import java.util.Arrays;

/** Flame Graph type query parameter. */
public class FlameGraphTypeQueryParameter
        extends MessageQueryParameter<FlameGraphTypeQueryParameter.Type> {

    private static final String key = "type";

    public FlameGraphTypeQueryParameter() {
        super(key, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public Type convertStringToValue(String value) {
        return Type.valueOf(value.toUpperCase());
    }

    @Override
    public String convertValueToString(Type value) {
        return value.name().toLowerCase();
    }

    @Override
    public String getDescription() {
        return "String value that specifies the Flame Graph type. Supported options are: \""
                + Arrays.toString(Type.values())
                + "\".";
    }

    /** Flame Graph type. */
    public enum Type {
        /** Type of the Flame Graph that includes threads in all possible states. */
        FULL,
        /** Type of the Flame Graph that includes threads in states Thread.State.[RUNNABLE, NEW]. */
        ON_CPU,
        /**
         * Type of the Flame Graph that includes threads in states Thread.State.[TIMED_WAITING,
         * BLOCKED, WAITING].
         */
        OFF_CPU
    }
}

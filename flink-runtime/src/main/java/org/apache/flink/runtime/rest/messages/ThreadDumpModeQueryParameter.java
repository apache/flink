/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages;

import java.util.Arrays;

/**
 * Optional {@code mode} query parameter for the thread-dump REST endpoints. When omitted, the
 * cluster default ({@code cluster.thread-dump.default-mode}) is used.
 */
public class ThreadDumpModeQueryParameter extends MessageQueryParameter<ThreadDumpMode> {

    public static final String KEY = "mode";

    public ThreadDumpModeQueryParameter() {
        super(KEY, MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public ThreadDumpMode convertStringToValue(String value) {
        // Case-insensitive; unknown values throw IllegalArgumentException, which the REST layer
        // translates into a 400 Bad Request.
        return ThreadDumpMode.valueOf(value.trim().toUpperCase());
    }

    @Override
    public String convertValueToString(ThreadDumpMode value) {
        return value.name().toLowerCase();
    }

    @Override
    public String getDescription() {
        return "Controls how much lock information is collected. Supported values: "
                + Arrays.toString(ThreadDumpMode.values())
                + ". When omitted, cluster.thread-dump.default-mode is used.";
    }
}

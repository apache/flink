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

package org.apache.flink.table.planner.hint;

import java.util.Locale;

/**
 * Hint strategy to configure different {@link
 * org.apache.flink.table.api.config.ExecutionConfigOptions#IDLE_STATE_RETENTION} for stream joins.
 *
 * <p>TODO support agg state ttl hint.
 */
public enum StateTtlHint {

    /**
     * Instructs the optimizer to use the specified state ttl for the underlying table.
     *
     * <p>Only accept key-value hint options.
     */
    STATE_TTL("STATE_TTL");

    private final String hintName;

    StateTtlHint(String hintName) {
        this.hintName = hintName;
    }

    public String getHintName() {
        return hintName;
    }

    public static boolean isStateTtlHint(String hintName) {
        try {
            String formalizedHintName = hintName.toUpperCase(Locale.ROOT);
            return StateTtlHint.valueOf(formalizedHintName) == STATE_TTL;
        } catch (Exception e) {
            return false;
        }
    }
}

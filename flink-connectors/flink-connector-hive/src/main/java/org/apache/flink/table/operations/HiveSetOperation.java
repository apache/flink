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

package org.apache.flink.table.operations;

import javax.annotation.Nullable;

import java.util.Optional;

/** Hive's set operation. */
public class HiveSetOperation implements Operation {
    private final @Nullable String key;
    private final @Nullable String value;
    // for Hive's command "set -v"
    private final boolean isVerbose;

    public HiveSetOperation() {
        this(null, null, false);
    }

    public HiveSetOperation(boolean isVerbose) {
        this(null, null, isVerbose);
    }

    public HiveSetOperation(String key) {
        this(key, null);
    }

    public HiveSetOperation(String key, String value) {
        this(key, value, false);
    }

    public HiveSetOperation(String key, String value, boolean isVerbose) {
        this.key = key;
        this.value = value;
        this.isVerbose = isVerbose;
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public Optional<String> getKey() {
        return Optional.ofNullable(key);
    }

    public Optional<String> getValue() {
        return Optional.ofNullable(value);
    }

    @Override
    public String asSummaryString() {
        if (isVerbose) {
            return "set -v";
        }
        StringBuilder sb = new StringBuilder("set");
        if (key != null) {
            sb.append(" ").append(key).append("=");
        }
        if (value != null) {
            sb.append(value);
        }
        return sb.toString();
    }
}

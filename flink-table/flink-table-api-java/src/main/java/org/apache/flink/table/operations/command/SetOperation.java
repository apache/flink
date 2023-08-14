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

package org.apache.flink.table.operations.command;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.Operation;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Operation to represent SET command. If {@link #getKey()} and {@link #getValue()} are empty, it
 * means show all the configurations. Otherwise, set value to the configuration key.
 */
@Internal
public class SetOperation implements Operation {

    @Nullable private final String key;
    @Nullable private final String value;

    public SetOperation() {
        this.key = null;
        this.value = null;
    }

    public SetOperation(String key, String value) {
        this.key = checkNotNull(key);
        this.value = checkNotNull(value);
    }

    public Optional<String> getKey() {
        return Optional.ofNullable(key);
    }

    public Optional<String> getValue() {
        return Optional.ofNullable(value);
    }

    @Override
    public String asSummaryString() {
        if (key == null && value == null) {
            return "SET";
        } else {
            return String.format("SET %s=%s", key, value);
        }
    }
}

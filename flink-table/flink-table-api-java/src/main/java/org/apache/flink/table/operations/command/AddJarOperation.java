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

import org.apache.flink.table.operations.Operation;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Operation to represent ADD JAR command. If {@link #getValue()} and {@link #getValue()} are empty,
 * it means show all the configurations. Otherwise, set value to the configuration key.
 */
public class AddJarOperation implements Operation {

    @Nullable private final String value;

    public AddJarOperation(String value) {
        this.value = checkNotNull(value);
    }

    public Optional<String> getValue() {
        return Optional.ofNullable(value);
    }

    @Override
    public String asSummaryString() {
        if (value == null) {
            return "ADD JAR";
        } else {
            return String.format("ADD JAR %s", value);
        }
    }
}

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

package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.ResetOperation;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link FlinkExtendedParser} is used for parsing some special command which should be converted to
 * Flink's {@link org.apache.flink.table.operations.Operation} and follow Flink's behavior.
 */
public class FlinkExtendedParser {
    private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
    private static final Pattern RESET_PATTERN =
            Pattern.compile("RESET(\\s+(?<key>[^'\\s]+)\\s*)?\\s*;?", DEFAULT_PATTERN_FLAGS);

    /** parse RESET statement. */
    public static Operation parseReset(String statement) {
        Matcher matcher = RESET_PATTERN.matcher(statement.trim());
        String key;

        if (matcher.find()) {
            key = matcher.group("key");
        } else {
            throw new TableException(
                    String.format(
                            "Failed to convert the statement to RESET operation: %s.", statement));
        }
        return new ResetOperation(key);
    }
}

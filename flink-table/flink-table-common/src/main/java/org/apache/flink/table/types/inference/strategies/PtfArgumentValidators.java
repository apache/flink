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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Optional;

/** Argument checks shared by the input type strategies of built-in process table functions. */
@Internal
public final class PtfArgumentValidators {

    /**
     * Returns an error if the {@code state_ttl} argument at {@code argPos} is neither NULL nor a
     * non-negative INTERVAL literal.
     */
    public static Optional<String> validateStateTtl(
            final CallContext callContext, final int argPos) {
        if (callContext.isArgumentNull(argPos)) {
            return Optional.empty();
        }
        if (!callContext.isArgumentLiteral(argPos)) {
            return Optional.of(
                    "The 'state_ttl' argument must be a constant INTERVAL literal, but received a "
                            + "non-literal expression. Use a literal such as INTERVAL '1' HOUR.");
        }
        return callContext
                .getArgumentValue(argPos, Duration.class)
                .filter(Duration::isNegative)
                .map(
                        stateTtl ->
                                String.format(
                                        "The 'state_ttl' argument must not be negative, but was "
                                                + "'%s'. Use INTERVAL '0' to retain state "
                                                + "indefinitely.",
                                        TimeUtils.formatWithHighestUnit(stateTtl)));
    }

    private PtfArgumentValidators() {}
}

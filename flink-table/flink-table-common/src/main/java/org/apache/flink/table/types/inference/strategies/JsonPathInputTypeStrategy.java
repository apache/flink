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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Strategy for a JSON function whose optional second argument is a path literal. Rejects the {@code
 * lax}/{@code strict} path mode prefix at planning time and delegates everything else to {@code
 * signatures}.
 */
@Internal
public final class JsonPathInputTypeStrategy implements InputTypeStrategy {

    private static final int ARG_PATH = 1;

    private static final Pattern PATH_MODE_PREFIX =
            Pattern.compile("\\s*(strict|lax)\\s+\\$.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final InputTypeStrategy signatures;

    JsonPathInputTypeStrategy(final InputTypeStrategy signatures) {
        this.signatures = signatures;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return signatures.getArgumentCount();
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            final CallContext callContext, final boolean throwOnFailure) {
        final Optional<List<DataType>> inferredDataTypes =
                signatures.inferInputTypes(callContext, throwOnFailure);
        if (inferredDataTypes.isEmpty() || callContext.getArgumentDataTypes().size() <= ARG_PATH) {
            return inferredDataTypes;
        }

        final Optional<String> path = callContext.getArgumentValue(ARG_PATH, String.class);
        if (path.isPresent() && PATH_MODE_PREFIX.matcher(path.get()).matches()) {
            return callContext.fail(
                    throwOnFailure,
                    "%s does not support the 'lax'/'strict' path mode prefix (got: '%s'). "
                            + "Use a plain path such as '$.a.b'. To check path existence or handle "
                            + "invalid input, use JSON_EXISTS or IS JSON.",
                    callContext.getName(),
                    path.get());
        }

        return inferredDataTypes;
    }

    @Override
    public List<Signature> getExpectedSignatures(final FunctionDefinition definition) {
        return signatures.getExpectedSignatures(definition);
    }
}

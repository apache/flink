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
import org.apache.flink.table.api.OverWindowRange;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Specific {@link InputTypeStrategy} for {@link BuiltInFunctionDefinitions#OVER}. */
@Internal
public class OverTypeStrategy implements InputTypeStrategy {
    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.from(4);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final DataType timeAttribute = argumentDataTypes.get(1);

        if (!LogicalTypeChecks.isTimeAttribute(timeAttribute.getLogicalType())) {
            return callContext.fail(
                    throwOnFailure,
                    "Second argument in OVER window must be a TIME ATTRIBUTE, but is: "
                            + timeAttribute);
        }

        if (!callContext.isArgumentLiteral(2)) {
            return callContext.fail(
                    throwOnFailure, "Preceding must be a row interval or time interval literal.");
        }

        if (!callContext.isArgumentLiteral(3)) {
            return callContext.fail(
                    throwOnFailure, "Following must be a row interval or time interval literal.");
        }

        final DataType preceding = argumentDataTypes.get(2);
        final DataType following = argumentDataTypes.get(3);

        if (preceding.getLogicalType().is(LogicalTypeRoot.NULL)
                || following.getLogicalType().is(LogicalTypeRoot.NULL)) {
            if (!preceding.getLogicalType().is(LogicalTypeRoot.NULL)
                    || !following.getLogicalType().is(LogicalTypeRoot.NULL)) {
                return callContext.fail(
                        throwOnFailure, "Both preceding and following must be provided" + ".");
            }

            return Optional.of(argumentDataTypes);
        }

        return validatePrecedingFollowingPresent(
                callContext, throwOnFailure, preceding, following, argumentDataTypes);
    }

    private Optional<List<DataType>> validatePrecedingFollowingPresent(
            CallContext callContext,
            boolean throwOnFailure,
            DataType preceding,
            DataType following,
            List<DataType> argumentDataTypes) {
        final LogicalTypeRoot precedingTypeRoot = preceding.getLogicalType().getTypeRoot();
        final Optional<String> precedingErr =
                validateFollowingPreceding(
                        precedingTypeRoot, FollowingPreceding.PRECEDING, callContext);

        if (precedingErr.isPresent()) {
            return callContext.fail(throwOnFailure, precedingErr.get());
        }

        final LogicalTypeRoot followingTypeRoot = following.getLogicalType().getTypeRoot();
        final Optional<String> followingErr =
                validateFollowingPreceding(
                        followingTypeRoot, FollowingPreceding.FOLLOWING, callContext);

        if (followingErr.isPresent()) {
            return callContext.fail(throwOnFailure, followingErr.get());
        }

        final boolean isPrecedingRowWindow =
                isRowWindow(callContext, precedingTypeRoot, FollowingPreceding.PRECEDING);
        final boolean isFollowingRowWindow =
                isRowWindow(callContext, followingTypeRoot, FollowingPreceding.FOLLOWING);

        if (isPrecedingRowWindow && !isFollowingRowWindow
                || !isPrecedingRowWindow && isFollowingRowWindow) {
            return callContext.fail(
                    throwOnFailure, "Preceding and following must be of same interval type.");
        }

        return Optional.of(argumentDataTypes);
    }

    private static boolean isRowWindow(
            CallContext callContext,
            LogicalTypeRoot typeRoot,
            FollowingPreceding followingPreceding) {
        switch (typeRoot) {
            case BIGINT:
                return true;
            case SYMBOL:
                final OverWindowRange windowRange =
                        callContext
                                .getArgumentValue(followingPreceding.pos, OverWindowRange.class)
                                .get();
                return windowRange == OverWindowRange.UNBOUNDED_ROW
                        || windowRange == OverWindowRange.CURRENT_ROW;
            default:
                return false;
        }
    }

    private enum FollowingPreceding {
        PRECEDING("Preceding", 2),
        FOLLOWING("Following", 3);

        private final int pos;
        private final String name;

        FollowingPreceding(String name, int pos) {
            this.name = name;
            this.pos = pos;
        }
    }

    private Optional<String> validateFollowingPreceding(
            LogicalTypeRoot typeRoot,
            FollowingPreceding followingPreceding,
            CallContext callContext) {
        switch (typeRoot) {
            case BIGINT:
                if (callContext.getArgumentValue(followingPreceding.pos, Long.class).get() <= 0) {
                    return Optional.of(
                            String.format(
                                    "%s row interval must be larger than 0.",
                                    followingPreceding.name));
                }
                break;
            case INTERVAL_DAY_TIME:
                if (callContext
                                .getArgumentValue(followingPreceding.pos, BigDecimal.class)
                                .get()
                                .compareTo(BigDecimal.ZERO)
                        < 0) {
                    return Optional.of(
                            String.format(
                                    "%s time interval must be equal or larger than 0.",
                                    followingPreceding.name));
                }
                break;
            case SYMBOL:
                final Optional<OverWindowRange> precedingSymbol =
                        callContext.getArgumentValue(followingPreceding.pos, OverWindowRange.class);
                if (!precedingSymbol.isPresent()) {
                    return Optional.of(
                            String.format(
                                    "%s must be a row interval or time interval literal.",
                                    followingPreceding.name));
                }
                break;
            default:
                return Optional.of(
                        String.format(
                                "%s must be a row interval or time interval literal.",
                                followingPreceding.name));
        }
        return Optional.empty();
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(Signature.of(Argument.ofGroupVarying("INTERNAL")));
    }
}

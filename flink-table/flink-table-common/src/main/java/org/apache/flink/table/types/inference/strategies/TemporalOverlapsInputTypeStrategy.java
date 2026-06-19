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
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Type strategy of {@code TO_TIMESTAMP_LTZ}. */
@Internal
class TemporalOverlapsInputTypeStrategy implements InputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(4);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> args = callContext.getArgumentDataTypes();
        final LogicalType leftTimePoint = args.get(0).getLogicalType();
        final LogicalType leftTemporal = args.get(1).getLogicalType();
        final LogicalType rightTimePoint = args.get(2).getLogicalType();
        final LogicalType rightTemporal = args.get(3).getLogicalType();

        if (!leftTimePoint.is(LogicalTypeFamily.DATETIME)) {
            return callContext.fail(
                    throwOnFailure,
                    "TEMPORAL_OVERLAPS requires 1st argument 'leftTimePoint' to be a DATETIME type, but is %s",
                    leftTimePoint);
        }
        if (!rightTimePoint.is(LogicalTypeFamily.DATETIME)) {
            return callContext.fail(
                    throwOnFailure,
                    "TEMPORAL_OVERLAPS requires 3rd argument 'rightTimePoint' to be a DATETIME type, but is %s",
                    rightTimePoint);
        }

        if (!leftTimePoint.equals(rightTimePoint)) {
            return callContext.fail(
                    throwOnFailure,
                    "TEMPORAL_OVERLAPS requires 'leftTimePoint' and 'rightTimePoint' arguments to be of the same type, but is %s != %s",
                    leftTimePoint,
                    rightTimePoint);
        }

        // leftTemporal is point, then it must be comparable with leftTimePoint
        if (leftTemporal.is(LogicalTypeFamily.DATETIME)) {
            if (!leftTemporal.equals(leftTimePoint)) {
                return callContext.fail(
                        throwOnFailure,
                        "TEMPORAL_OVERLAPS requires 'leftTemporal' and 'leftTimePoint' arguments to be of the same type if 'leftTemporal' is a DATETIME, but is %s != %s",
                        leftTemporal,
                        leftTimePoint);
            }
        } else if (!leftTemporal.is(LogicalTypeFamily.INTERVAL)) {
            return callContext.fail(
                    throwOnFailure,
                    "TEMPORAL_OVERLAPS requires 2nd argument 'leftTemporal' to be DATETIME or INTERVAL type, but is %s",
                    leftTemporal);
        }

        // rightTemporal is point, then it must be comparable with rightTimePoint
        if (rightTemporal.is(LogicalTypeFamily.DATETIME)) {
            if (!rightTemporal.equals(rightTimePoint)) {
                return callContext.fail(
                        throwOnFailure,
                        "TEMPORAL_OVERLAPS requires 'rightTemporal' and 'rightTimePoint' arguments to be of the same type if 'rightTemporal' is a DATETIME, but is %s != %s",
                        rightTemporal,
                        rightTimePoint);
            }
        } else if (!rightTemporal.is(LogicalTypeFamily.INTERVAL)) {
            return callContext.fail(
                    throwOnFailure,
                    "TEMPORAL_OVERLAPS requires 4th argument 'rightTemporal' to be DATETIME or INTERVAL type, but is %s",
                    rightTemporal);
        }

        return Optional.of(args);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(
                        Argument.ofGroup("leftTimePoint", LogicalTypeFamily.DATETIME),
                        Argument.ofGroup("leftTemporal", "TEMPORAL"),
                        Argument.ofGroup("rightTimePoint", LogicalTypeFamily.DATETIME),
                        Argument.ofGroup("rightTemporal", "TEMPORAL")));
    }
}

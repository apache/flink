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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Utilities for recognizing calls to the {@code SNAPSHOT} built-in used by the {@code LATERAL
 * SNAPSHOT} processing-time temporal join.
 */
@Internal
public final class LateralSnapshotJoinUtil {

    /**
     * {@code true} when {@code definition} is the {@link BuiltInFunctionDefinitions#SNAPSHOT}
     * built-in.
     */
    public static boolean isSnapshotFunction(@Nullable FunctionDefinition definition) {
        return definition instanceof BuiltInFunctionDefinition
                && BuiltInFunctionDefinitions.SNAPSHOT
                        .getName()
                        .equals(((BuiltInFunctionDefinition) definition).getName());
    }

    /**
     * {@code true} when the operator of {@code call} is a {@link BridgingSqlFunction} whose
     * function definition is the SNAPSHOT built-in.
     */
    public static boolean isSnapshotCall(@Nullable RexCall call) {
        if (call == null) {
            return false;
        }
        if (!(call.getOperator() instanceof BridgingSqlFunction)) {
            return false;
        }
        final BridgingSqlFunction bridging = (BridgingSqlFunction) call.getOperator();
        return isSnapshotFunction(bridging.getDefinition());
    }

    /**
     * Derives the output row type of a {@code LATERAL SNAPSHOT} join. The {@code SNAPSHOT} function
     * does not forward the build-side (right) time attributes, so they are materialized in the
     * output; the probe-side (left) time attributes are forwarded unchanged. Field names are
     * uniquified and the build-side nullability follows the join type, matching {@link
     * org.apache.calcite.rel.core.Join#deriveRowType()}.
     */
    public static RelDataType deriveRowType(
            RelDataTypeFactory typeFactory,
            RelDataType leftType,
            RelDataType rightType,
            JoinRelType joinType,
            List<RelDataTypeField> systemFieldList) {
        final RelDataTypeFactory.Builder materializedRight = typeFactory.builder();
        for (RelDataTypeField field : rightType.getFieldList()) {
            final RelDataType fieldType =
                    field.getType() instanceof TimeIndicatorRelDataType
                            ? ((TimeIndicatorRelDataType) field.getType()).getOriginalType()
                            : field.getType();
            materializedRight.add(field.getName(), fieldType);
        }
        return SqlValidatorUtil.deriveJoinRowType(
                leftType, materializedRight.build(), joinType, typeFactory, null, systemFieldList);
    }

    private LateralSnapshotJoinUtil() {}
}

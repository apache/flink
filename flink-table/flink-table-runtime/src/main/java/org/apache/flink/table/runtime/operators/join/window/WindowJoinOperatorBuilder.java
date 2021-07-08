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

package org.apache.flink.table.runtime.operators.join.window;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import java.time.ZoneId;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link WindowJoinOperatorBuilder} is used to build a {@link WindowJoinOperator} for window
 * join.
 *
 * <pre>
 * WindowJoinOperatorBuilder.builder()
 *   .leftType(leftType)
 *   .rightType(rightType)
 *   .generatedJoinCondition(generatedJoinCondition)
 *   .leftWindowEndIndex(leftWindowEndIndex)
 *   .rightWindowEndIndex(rightWindowEndIndex)
 *   .filterNullKeys(filterNullKeys)
 *   .joinType(joinType)
 *   .build();
 * </pre>
 */
public class WindowJoinOperatorBuilder {

    public static WindowJoinOperatorBuilder builder() {
        return new WindowJoinOperatorBuilder();
    }

    private TypeSerializer<RowData> leftSerializer;
    private TypeSerializer<RowData> rightSerializer;
    private GeneratedJoinCondition generatedJoinCondition;
    private int leftWindowEndIndex = -1;
    private int rightWindowEndIndex = -1;
    private boolean[] filterNullKeys;
    private FlinkJoinType joinType;
    private ZoneId shiftTimeZone = ZoneId.of("UTC");

    public WindowJoinOperatorBuilder leftSerializer(TypeSerializer<RowData> leftSerializer) {
        this.leftSerializer = leftSerializer;
        return this;
    }

    public WindowJoinOperatorBuilder rightSerializer(TypeSerializer<RowData> rightSerializer) {
        this.rightSerializer = rightSerializer;
        return this;
    }

    public WindowJoinOperatorBuilder generatedJoinCondition(
            GeneratedJoinCondition generatedJoinCondition) {
        this.generatedJoinCondition = generatedJoinCondition;
        return this;
    }

    public WindowJoinOperatorBuilder filterNullKeys(boolean[] filterNullKeys) {
        this.filterNullKeys = filterNullKeys;
        return this;
    }

    public WindowJoinOperatorBuilder joinType(FlinkJoinType joinType) {
        this.joinType = joinType;
        return this;
    }

    public WindowJoinOperatorBuilder leftWindowEndIndex(int leftWindowEndIndex) {
        this.leftWindowEndIndex = leftWindowEndIndex;
        return this;
    }

    public WindowJoinOperatorBuilder rightWindowEndIndex(int rightWindowEndIndex) {
        this.rightWindowEndIndex = rightWindowEndIndex;
        return this;
    }

    /**
     * The shift timezone of the window, if the proctime or rowtime type is TIMESTAMP_LTZ, the shift
     * timezone is the timezone user configured in TableConfig, other cases the timezone is UTC
     * which means never shift when assigning windows.
     */
    public WindowJoinOperatorBuilder withShiftTimezone(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
        return this;
    }

    public WindowJoinOperator build() {
        checkNotNull(leftSerializer);
        checkNotNull(rightSerializer);
        checkNotNull(generatedJoinCondition);
        checkNotNull(filterNullKeys);
        checkNotNull(joinType);

        checkArgument(
                leftWindowEndIndex >= 0,
                String.format(
                        "Illegal window end index %s, it should not be negative!",
                        leftWindowEndIndex));
        checkArgument(
                rightWindowEndIndex >= 0,
                String.format(
                        "Illegal window end index %s, it should not be negative!",
                        rightWindowEndIndex));

        switch (joinType) {
            case INNER:
                return new WindowJoinOperator.InnerJoinOperator(
                        leftSerializer,
                        rightSerializer,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys,
                        shiftTimeZone);
            case SEMI:
                return new WindowJoinOperator.SemiAntiJoinOperator(
                        leftSerializer,
                        rightSerializer,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys,
                        false,
                        shiftTimeZone);
            case ANTI:
                return new WindowJoinOperator.SemiAntiJoinOperator(
                        leftSerializer,
                        rightSerializer,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys,
                        true,
                        shiftTimeZone);
            case LEFT:
                return new WindowJoinOperator.LeftOuterJoinOperator(
                        leftSerializer,
                        rightSerializer,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys,
                        shiftTimeZone);
            case RIGHT:
                return new WindowJoinOperator.RightOuterJoinOperator(
                        leftSerializer,
                        rightSerializer,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys,
                        shiftTimeZone);
            case FULL:
                return new WindowJoinOperator.FullOuterJoinOperator(
                        leftSerializer,
                        rightSerializer,
                        generatedJoinCondition,
                        leftWindowEndIndex,
                        rightWindowEndIndex,
                        filterNullKeys,
                        shiftTimeZone);
            default:
                throw new IllegalArgumentException("Invalid join type: " + joinType);
        }
    }
}

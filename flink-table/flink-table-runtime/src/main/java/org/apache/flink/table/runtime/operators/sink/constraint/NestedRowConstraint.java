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

package org.apache.flink.table.runtime.operators.sink.constraint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.UpdatableRowData;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Checks constraints on nested rows and structured types. */
@Internal
final class NestedRowConstraint implements Constraint {

    private final int[] nestedRowFieldIndices;
    private final int[] nestedRowFieldArities;
    private final String[] nestedRowFieldNames;
    private final Constraint[][] nestedRowConstraints;

    NestedRowConstraint(
            int[] nestedRowFieldIndices,
            int[] nestedRowFieldArities,
            String[] nestedRowFieldNames,
            Constraint[][] nestedRowConstraints) {
        this.nestedRowFieldIndices = nestedRowFieldIndices;
        this.nestedRowFieldArities = nestedRowFieldArities;
        this.nestedRowFieldNames = nestedRowFieldNames;
        this.nestedRowConstraints = nestedRowConstraints;
    }

    @Nullable
    @Override
    public RowData enforce(RowData input) {
        UpdatableRowData updatableRowData = null;
        for (int i = 0; i < nestedRowFieldIndices.length; i++) {
            final int index = nestedRowFieldIndices[i];
            if (!input.isNullAt(index)) {
                RowData nestedRow = input.getRow(index, nestedRowFieldArities[i]);
                for (Constraint constraint : nestedRowConstraints[i]) {
                    RowData enforcedRow = enforce(constraint, nestedRow, i);
                    if (enforcedRow == null) {
                        return null;
                    }
                    if (enforcedRow != nestedRow) {
                        if (updatableRowData == null) {
                            updatableRowData = new UpdatableRowData(input, input.getArity());
                        }
                        updatableRowData.setField(index, enforcedRow);
                        nestedRow = enforcedRow;
                    }
                }
            }
        }
        return updatableRowData != null ? updatableRowData : input;
    }

    private RowData enforce(Constraint constraint, RowData nestedRow, int index) {
        try {
            return constraint.enforce(nestedRow);
        } catch (EnforcerException e) {
            throw new EnforcerException(
                    e.getFormat(), nestedRowFieldNames[index] + "." + e.getColumnName());
        }
    }

    @Override
    public String toString() {
        return String.format(
                "NestedRowEnforcer(constraints=[%s])",
                IntStream.range(0, nestedRowFieldIndices.length)
                        .mapToObj(
                                idx ->
                                        String.format(
                                                "{%s=%s}",
                                                nestedRowFieldNames[idx],
                                                Arrays.toString(nestedRowConstraints[idx])))
                        .collect(Collectors.joining(", ")));
    }
}

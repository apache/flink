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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Checks constraints on nested arrays. */
@Internal
final class NestedArrayConstraint implements Constraint {

    private final int[] nestedArrayFieldIndices;
    private final String[] nestedArrayFieldNames;
    private final Constraint[][] nestedElementsConstraints;
    private final ArrayData.ElementGetter[] elementGetters;

    NestedArrayConstraint(
            final int[] nestedArrayFieldIndices,
            final String[] nestedArrayFieldNames,
            final Constraint[][] nestedElementsConstraints,
            final ArrayData.ElementGetter[] elementGetters) {
        this.nestedArrayFieldIndices = nestedArrayFieldIndices;
        this.nestedArrayFieldNames = nestedArrayFieldNames;
        this.nestedElementsConstraints = nestedElementsConstraints;
        this.elementGetters = elementGetters;
    }

    @Nullable
    @Override
    public RowData enforce(RowData input) {
        for (int i = 0; i < nestedArrayFieldIndices.length; i++) {
            final int index = nestedArrayFieldIndices[i];
            if (!input.isNullAt(index)) {
                ArrayData nestedArray = input.getArray(index);
                final Constraint[] nestedConstraints = nestedElementsConstraints[i];
                final ArrayData.ElementGetter elementGetter = elementGetters[i];
                for (int entryIdx = 0; entryIdx < nestedArray.size(); entryIdx++) {
                    final Object element = elementGetter.getElementOrNull(nestedArray, entryIdx);
                    for (Constraint nestedConstraint : nestedConstraints) {
                        if (enforce(nestedConstraint, element, i, entryIdx) == null) {
                            // the record is invalid
                            return null;
                        }
                    }
                }
            }
        }
        return input;
    }

    private RowData enforce(Constraint nestedConstraint, Object element, int i, int entryIdx) {
        try {
            return nestedConstraint.enforce(GenericRowData.of(element));
        } catch (EnforcerException e) {
            final String nestedColumnName = e.getColumnName().replace("element", "");
            String columnName = String.format("%s[%d]", nestedArrayFieldNames[i], entryIdx);
            if (!StringUtils.isNullOrWhitespaceOnly(nestedColumnName)) {
                columnName += nestedColumnName;
            }
            throw new EnforcerException(e.getFormat(), columnName);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "NestedArrayEnforcer(constraints=[%s])",
                IntStream.range(0, nestedArrayFieldIndices.length)
                        .mapToObj(
                                idx ->
                                        String.format(
                                                "{%s=%s}",
                                                nestedArrayFieldNames[idx],
                                                Arrays.toString(nestedElementsConstraints[idx])))
                        .collect(Collectors.joining(", ")));
    }
}

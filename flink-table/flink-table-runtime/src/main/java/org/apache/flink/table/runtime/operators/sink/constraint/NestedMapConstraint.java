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
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Checks constraints on nested maps. */
@Internal
final class NestedMapConstraint implements Constraint {

    private final int[] nestedMapFieldIndices;
    private final String[] nestedMapFieldNames;
    private final Constraint[][] nestedElementsConstraints;
    private final ArrayData.ElementGetter[] valueGetters;
    private final ArrayData.ElementGetter[] keyGetters;

    NestedMapConstraint(
            final int[] nestedMapFieldIndices,
            final String[] nestedMapFieldNames,
            final Constraint[][] nestedElementsConstraints,
            final ArrayData.ElementGetter[] valueGetters,
            final ArrayData.ElementGetter[] keyGetters) {
        this.nestedMapFieldIndices = nestedMapFieldIndices;
        this.nestedMapFieldNames = nestedMapFieldNames;
        this.nestedElementsConstraints = nestedElementsConstraints;
        this.valueGetters = valueGetters;
        this.keyGetters = keyGetters;
    }

    @Nullable
    @Override
    public RowData enforce(RowData input) {
        for (int i = 0; i < nestedMapFieldIndices.length; i++) {
            final int index = nestedMapFieldIndices[i];
            if (!input.isNullAt(index)) {
                MapData nestedMap = input.getMap(index);
                final Constraint[] nestedConstraints = nestedElementsConstraints[i];
                final ArrayData.ElementGetter keyGetter = keyGetters[i];
                final ArrayData.ElementGetter valueGetter = valueGetters[i];
                final ArrayData keys = nestedMap.keyArray();
                final ArrayData values = nestedMap.valueArray();
                for (int entryIdx = 0; entryIdx < nestedMap.size(); entryIdx++) {
                    final Object key = keyGetter.getElementOrNull(keys, entryIdx);
                    final Object value = valueGetter.getElementOrNull(values, entryIdx);
                    for (Constraint nestedConstraint : nestedConstraints) {
                        if (enforce(nestedConstraint, key, value, i) == null) {
                            return null;
                        }
                    }
                }
            }
        }
        return input;
    }

    private RowData enforce(Constraint nestedConstraint, Object key, Object value, int i) {
        try {
            return nestedConstraint.enforce(GenericRowData.of(key, value));
        } catch (EnforcerException e) {
            final String adjustedNestedColumnName = getAdjustedNestedColumnName(e);
            throw new EnforcerException(
                    e.getFormat(),
                    String.format("%s[%s", nestedMapFieldNames[i], adjustedNestedColumnName));
        }
    }

    private static String getAdjustedNestedColumnName(EnforcerException e) {
        final String adjustedNestedColumnName;
        if ("key".equals(e.getColumnName()) || "value".equals(e.getColumnName())) {
            adjustedNestedColumnName = e.getColumnName() + "]";
        } else {
            adjustedNestedColumnName =
                    e.getColumnName()
                            // the column will start with either "key." or "value."
                            // replace so that we
                            // get <column_name>[key/value].<nested_column_name>
                            .replaceFirst("\\.", "]");
        }
        return adjustedNestedColumnName;
    }

    @Override
    public String toString() {
        return String.format(
                "NestedMapEnforcer(constraints=[%s])",
                IntStream.range(0, nestedMapFieldIndices.length)
                        .mapToObj(
                                idx ->
                                        String.format(
                                                "{%s==%s}",
                                                nestedMapFieldNames[idx],
                                                Arrays.toString(nestedElementsConstraints[idx])))
                        .collect(Collectors.joining(", ")));
    }
}

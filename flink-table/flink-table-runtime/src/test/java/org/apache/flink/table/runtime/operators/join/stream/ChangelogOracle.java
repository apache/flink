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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/**
 * Materializes the changelog that an operator emits and checks that it is valid.
 *
 * <ul>
 *   <li>Valid changelog: every -U/-D retracts a row that was emitted before and not retracted
 *       since, so no row count goes below zero. A -D whose fields are null may contain only a key:
 *       it retracts the row whose non-null fields equal, if there is no exact match.
 *   <li>Correct result: the materialized result equals the expected one.
 * </ul>
 *
 * <p>Without key fields, the result is a multiset of rows (a retract stream). With key fields, the
 * changelog is materialized like an upsert sink with these primary key fields does: +I/+U replace
 * the row of their key, -U/-D delete the row of their key. A valid changelog then requires that a
 * -U/-D retracts the current row of its key; a violation is still applied, as the sink would.
 */
public final class ChangelogOracle {

    private final RowData.FieldGetter[] fieldGetters;

    private final int[] keyFields;

    /** Materialized rows and their counts, in insertion order, without key fields. */
    private final Map<List<Object>, Integer> rows = new LinkedHashMap<>();

    /** Materialized rows by their key, with key fields. */
    private final Map<List<Object>, List<Object>> rowsByKey = new LinkedHashMap<>();

    private final List<String> violations = new ArrayList<>();

    private final List<String> changelog = new ArrayList<>();

    public ChangelogOracle(RowType outputType, int... keyFields) {
        final List<LogicalType> types = outputType.getChildren();
        this.fieldGetters = new RowData.FieldGetter[types.size()];
        for (int i = 0; i < types.size(); i++) {
            fieldGetters[i] = RowData.createFieldGetter(types.get(i), i);
        }
        this.keyFields = keyFields;
    }

    /** Takes the records emitted so far from the output queue of a test harness. */
    public void consume(Queue<Object> output) {
        Object element;
        while ((element = output.poll()) != null) {
            if (element instanceof StreamRecord) {
                apply((RowData) ((StreamRecord<?>) element).getValue());
            }
        }
    }

    public void apply(RowData row) {
        final List<Object> fields = fields(row);
        final RowKind kind = row.getRowKind();
        changelog.add(kind.shortString() + fields);
        if (keyFields.length > 0) {
            applyByKey(kind, fields);
            return;
        }
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            rows.merge(fields, 1, Integer::sum);
            return;
        }
        List<Object> retracted = rows.containsKey(fields) ? fields : null;
        if (retracted == null && kind == RowKind.DELETE) {
            retracted =
                    rows.keySet().stream()
                            .filter(r -> matchesKeyOnly(fields, r))
                            .findFirst()
                            .orElse(null);
        }
        if (retracted == null) {
            violations.add(
                    String.format(
                            "#%d %s%s retracts a row that is not in the result %s",
                            changelog.size(), kind.shortString(), fields, rows()));
            return;
        }
        rows.computeIfPresent(retracted, (r, count) -> count == 1 ? null : count - 1);
    }

    private void applyByKey(RowKind kind, List<Object> fields) {
        final List<Object> key = new ArrayList<>(keyFields.length);
        for (int keyField : keyFields) {
            key.add(fields.get(keyField));
        }
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            rowsByKey.put(key, fields);
            return;
        }
        final List<Object> current = rowsByKey.remove(key);
        if (current == null) {
            violations.add(
                    String.format(
                            "#%d %s%s retracts key %s that is not in the result %s",
                            changelog.size(), kind.shortString(), fields, key, rows()));
        } else if (!current.equals(fields)
                && !(kind == RowKind.DELETE && matchesKeyOnly(fields, current))) {
            violations.add(
                    String.format(
                            "#%d %s%s retracts key %s, whose row in the result is %s",
                            changelog.size(), kind.shortString(), fields, key, current));
        }
    }

    /** The materialized rows, with a row of count n repeated n times. */
    public List<List<Object>> rows() {
        if (keyFields.length > 0) {
            return new ArrayList<>(rowsByKey.values());
        }
        final List<List<Object>> result = new ArrayList<>();
        rows.forEach(
                (row, count) -> {
                    for (int i = 0; i < count; i++) {
                        result.add(row);
                    }
                });
        return result;
    }

    public List<String> violations() {
        return violations;
    }

    public String changelog() {
        return String.join(", ", changelog);
    }

    /** Asserts that the changelog so far is valid. */
    public void assertValidChangelog() {
        assertThat(violations).as("violations of changelog [%s]", changelog()).isEmpty();
    }

    /**
     * Asserts that the changelog so far is valid and that its materialized result equals the
     * expected rows, reporting both.
     */
    public void assertResult(Object[]... expectedRows) {
        final List<List<Object>> expected =
                Arrays.stream(expectedRows).map(Arrays::asList).collect(Collectors.toList());
        assertSoftly(
                softly -> {
                    softly.assertThat(violations)
                            .as("violations of changelog [%s]", changelog())
                            .isEmpty();
                    softly.assertThat(rows())
                            .as("materialized result of changelog [%s]", changelog())
                            .containsExactlyInAnyOrderElementsOf(expected);
                });
    }

    private List<Object> fields(RowData row) {
        final List<Object> fields = new ArrayList<>(fieldGetters.length);
        for (RowData.FieldGetter getter : fieldGetters) {
            final Object field = getter.getFieldOrNull(row);
            fields.add(field == null ? null : normalize(field));
        }
        return fields;
    }

    /** Strings are compared without the padding of CHAR fields. */
    private static Object normalize(Object field) {
        if (field instanceof Number || field instanceof Boolean) {
            return field;
        }
        return field.toString().trim();
    }

    private static boolean matchesKeyOnly(List<Object> keyOnlyDelete, List<Object> row) {
        final Iterator<Object> it = row.iterator();
        for (Object field : keyOnlyDelete) {
            final Object other = it.next();
            if (field != null && !Objects.equals(field, other)) {
                return false;
            }
        }
        return true;
    }

    public static Object[] row(Object... fields) {
        return fields;
    }
}

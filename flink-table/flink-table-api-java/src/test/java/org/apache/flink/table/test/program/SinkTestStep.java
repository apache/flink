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

package org.apache.flink.table.test.program;

import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Test step for creating a table sink. */
public final class SinkTestStep extends TableTestStep {

    public final @Nullable List<Row> expectedBeforeRestore;
    public final @Nullable List<Row> expectedAfterRestore;
    public final @Nullable List<String> expectedBeforeRestoreStrings;
    public final @Nullable List<String> expectedAfterRestoreStrings;
    // These are added for situations where we need to specify the output in batch.
    // In many cases, the "expectBeforeRestore*" variables are sufficient.
    public final @Nullable List<Row> expectedMaterializedRows;
    public final @Nullable List<String> expectedMaterializedStrings;
    public final boolean testChangelogData;
    public final @Nullable int[] deduplicatedFieldIndices;

    SinkTestStep(
            String name,
            List<String> schemaComponents,
            @Nullable TableDistribution distribution,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable List<Row> expectedBeforeRestore,
            @Nullable List<Row> expectedAfterRestore,
            @Nullable List<String> expectedBeforeRestoreStrings,
            @Nullable List<String> expectedAfterRestoreStrings,
            @Nullable List<Row> expectedMaterializedRows,
            @Nullable List<String> expectedMaterializedStrings,
            boolean testChangelogData,
            @Nullable int[] deduplicatedFieldIndices) {
        super(
                name,
                schemaComponents,
                distribution,
                partitionKeys,
                options,
                Collections.emptyList());
        if (hasRowsSet() && hasStringsSet()) {
            throw new IllegalArgumentException(
                    "You can not mix Row/String representations in restore data.");
        }
        this.expectedBeforeRestore = expectedBeforeRestore;
        this.expectedAfterRestore = expectedAfterRestore;
        this.expectedBeforeRestoreStrings = expectedBeforeRestoreStrings;
        this.expectedAfterRestoreStrings = expectedAfterRestoreStrings;
        this.expectedMaterializedRows = expectedMaterializedRows;
        this.expectedMaterializedStrings = expectedMaterializedStrings;
        this.testChangelogData = testChangelogData;
        this.deduplicatedFieldIndices = deduplicatedFieldIndices;

        if (deduplicatedFieldIndices != null && !hasRowsSet()) {
            throw new IllegalArgumentException(
                    "DeduplicatedFieldIndices can only be used for Row representations in restore data.");
        }
    }

    /** Builder for creating a {@link SinkTestStep}. */
    public static SinkTestStep.Builder newBuilder(String name) {
        return new SinkTestStep.Builder(name);
    }

    public List<String> getExpectedBeforeRestoreAsStrings() {
        if (expectedBeforeRestoreStrings != null) {
            return expectedBeforeRestoreStrings;
        }

        if (expectedBeforeRestore != null) {
            return expectedBeforeRestore.stream().map(Row::toString).collect(Collectors.toList());
        }

        return Collections.emptyList();
    }

    public List<String> getExpectedAfterRestoreAsStrings() {
        if (expectedAfterRestoreStrings != null) {
            return expectedAfterRestoreStrings;
        }

        if (expectedAfterRestore != null) {
            return expectedAfterRestore.stream().map(Row::toString).collect(Collectors.toList());
        }

        return Collections.emptyList();
    }

    public List<String> getExpectedAsStrings() {
        if (hasStringsSet() || deduplicatedFieldIndices == null) {
            final List<String> data = new ArrayList<>(getExpectedBeforeRestoreAsStrings());
            data.addAll(getExpectedAfterRestoreAsStrings());
            return data;
        }
        if (!hasRowsSet()) {
            return Collections.emptyList();
        }
        final List<Row> data = new ArrayList<>();
        if (expectedBeforeRestore != null) {
            data.addAll(expectedBeforeRestore);
        }
        if (expectedAfterRestore != null) {
            data.addAll(expectedAfterRestore);
        }

        Map<List<Object>, Row> deduplicatedMap = new HashMap<>();
        for (Row row : data) {
            List<Object> key = new ArrayList<>(deduplicatedFieldIndices.length);
            for (int deduplicatedFieldIndex : deduplicatedFieldIndices) {
                key.add(row.getField(deduplicatedFieldIndex));
            }
            deduplicatedMap.put(key, row);
        }
        return deduplicatedMap.values().stream().map(Row::toString).collect(Collectors.toList());
    }

    public List<String> getExpectedMaterializedResultsAsStrings() {
        if (expectedMaterializedStrings != null) {
            return expectedMaterializedStrings;
        }
        if (expectedMaterializedRows != null) {
            if (deduplicatedFieldIndices != null) {
                throw new UnsupportedOperationException(
                        "Unsupported to deduplicate data for materialized rows");
            }
            return expectedMaterializedRows.stream()
                    .map(Row::toString)
                    .collect(Collectors.toList());
        }
        return getExpectedAsStrings();
    }

    @Override
    public TestKind getKind() {
        return expectedBeforeRestore == null && expectedBeforeRestoreStrings == null
                ? TestKind.SINK_WITHOUT_DATA
                : expectedAfterRestore == null && expectedAfterRestoreStrings == null
                        ? TestKind.SINK_WITH_DATA
                        : TestKind.SINK_WITH_RESTORE_DATA;
    }

    public boolean shouldTestChangelogData() {
        return testChangelogData;
    }

    private boolean hasRowsSet() {
        return expectedBeforeRestore != null
                || expectedAfterRestore != null
                || expectedMaterializedRows != null;
    }

    private boolean hasStringsSet() {
        return expectedBeforeRestoreStrings != null
                || expectedAfterRestoreStrings != null
                || expectedMaterializedStrings != null;
    }

    /** Builder pattern for {@link SinkTestStep}. */
    public static final class Builder extends AbstractBuilder<Builder> {

        private List<Row> expectedBeforeRestore;
        private List<Row> expectedAfterRestore;
        private List<Row> expectedMaterializedBeforeRows;
        private List<String> expectedMaterializedBeforeStrings;

        private List<String> expectedBeforeRestoreStrings;
        private List<String> expectedAfterRestoreStrings;

        private boolean testChangelogData = true;

        private @Nullable int[] deduplicatedFieldIndices;

        private Builder(String name) {
            super(name);
        }

        public Builder consumedValues(Row... expectedRows) {
            return consumedBeforeRestore(expectedRows);
        }

        public Builder consumedValues(String... expectedRows) {
            return consumedBeforeRestore(expectedRows);
        }

        public Builder consumedBeforeRestore(Row... expectedRows) {
            this.expectedBeforeRestore = Arrays.asList(expectedRows);
            return this;
        }

        public Builder consumedBeforeRestore(String... expectedRows) {
            this.expectedBeforeRestoreStrings = Arrays.asList(expectedRows);
            return this;
        }

        public Builder consumedAfterRestore(Row... expectedRows) {
            this.expectedAfterRestore = Arrays.asList(expectedRows);
            return this;
        }

        public Builder consumedAfterRestore(String... expectedRows) {
            this.expectedAfterRestoreStrings = Arrays.asList(expectedRows);
            return this;
        }

        public Builder expectedMaterializedRows(Row... expectedRows) {
            this.expectedMaterializedBeforeRows = Arrays.asList(expectedRows);
            return this;
        }

        public Builder expectedMaterializedStrings(String... expectedRows) {
            this.expectedMaterializedBeforeStrings = Arrays.asList(expectedRows);
            return this;
        }

        public Builder testChangelogData() {
            this.testChangelogData = true;
            return this;
        }

        public Builder testMaterializedData() {
            this.testChangelogData = false;
            return this;
        }

        public Builder deduplicatedFieldIndices(int[] deduplicatedFieldIndices) {
            // TODO FLINK-38518 use pk to deduplicate data rather than specific fields.
            //  This task requires refactoring the current `AbstractBuilder` to separate the
            //  declaration of the primary key from the `List<String> schemaComponents`.
            this.deduplicatedFieldIndices = deduplicatedFieldIndices;
            return this;
        }

        public SinkTestStep build() {
            return new SinkTestStep(
                    name,
                    schemaComponents,
                    distribution,
                    partitionKeys,
                    options,
                    expectedBeforeRestore,
                    expectedAfterRestore,
                    expectedBeforeRestoreStrings,
                    expectedAfterRestoreStrings,
                    expectedMaterializedBeforeRows,
                    expectedMaterializedBeforeStrings,
                    testChangelogData,
                    deduplicatedFieldIndices);
        }
    }
}

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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Test step for creating a table sink. */
public final class SinkTestStep extends TableTestStep {

    public final @Nullable List<Row> expectedBeforeRestore;
    public final @Nullable List<Row> expectedAfterRestore;
    public final @Nullable List<String> expectedBeforeRestoreStrings;
    public final @Nullable List<String> expectedAfterRestoreStrings;
    public final boolean testChangelogData;

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
            boolean testChangelogData) {
        super(name, schemaComponents, distribution, partitionKeys, options);
        if (expectedBeforeRestore != null && expectedAfterRestoreStrings != null) {
            throw new IllegalArgumentException(
                    "You can not mix Row/String representation in before/after restore data.");
        }
        if (expectedBeforeRestoreStrings != null && expectedAfterRestore != null) {
            throw new IllegalArgumentException(
                    "You can not mix Row/String representation in before/after restore data.");
        }
        this.expectedBeforeRestore = expectedBeforeRestore;
        this.expectedAfterRestore = expectedAfterRestore;
        this.expectedBeforeRestoreStrings = expectedBeforeRestoreStrings;
        this.expectedAfterRestoreStrings = expectedAfterRestoreStrings;
        this.testChangelogData = testChangelogData;
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
        final List<String> data = new ArrayList<>(getExpectedBeforeRestoreAsStrings());
        data.addAll(getExpectedAfterRestoreAsStrings());
        return data;
    }

    @Override
    public TestKind getKind() {
        return expectedBeforeRestore == null && expectedBeforeRestoreStrings == null
                ? TestKind.SINK_WITHOUT_DATA
                : expectedAfterRestore == null && expectedAfterRestoreStrings == null
                        ? TestKind.SINK_WITH_DATA
                        : TestKind.SINK_WITH_RESTORE_DATA;
    }

    public boolean getTestChangelogData() {
        return testChangelogData;
    }

    /** Builder pattern for {@link SinkTestStep}. */
    public static final class Builder extends AbstractBuilder<Builder> {

        private List<Row> expectedBeforeRestore;
        private List<Row> expectedAfterRestore;

        private List<String> expectedBeforeRestoreStrings;
        private List<String> expectedAfterRestoreStrings;

        private boolean testChangelogData = true;

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

        public Builder testChangelogData() {
            this.testChangelogData = true;
            return this;
        }

        public Builder testMaterializedData() {
            this.testChangelogData = false;
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
                    testChangelogData);
        }
    }
}

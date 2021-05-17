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

package org.apache.flink.table.sources;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

/** Collection of tests that verify assumptions that table sources should meet. */
public abstract class TableSourceTestBase {

    /**
     * Constructs a table source to be tested.
     *
     * @param requestedSchema A requested schema for the table source. Some tests require particular
     *     behavior depending on the schema of a source.
     * @return table source to be tested
     */
    protected abstract TableSource<?> createTableSource(TableSchema requestedSchema);

    /**
     * Checks that {@link ProjectableTableSource#projectFields(int[])} returns a table source with a
     * different {@link TableSource#explainSource()} even when filtering out all fields.
     *
     * <p>Required by {@code PushProjectIntoTableSourceScanRule}.
     */
    @Test
    public void testEmptyProjection() {
        TableSource<?> source =
                createTableSource(TableSchema.builder().field("f0", DataTypes.INT()).build());
        assumeThat(source, instanceOf(ProjectableTableSource.class));

        ProjectableTableSource<?> projectableTableSource = (ProjectableTableSource<?>) source;

        TableSource<?> newTableSource = projectableTableSource.projectFields(new int[0]);
        assertThat(newTableSource.explainSource(), not(equalTo(source.explainSource())));
    }

    /**
     * Checks that {@link ProjectableTableSource#projectFields(int[])} returns a table source with a
     * different {@link TableSource#explainSource()}, but same schema.
     *
     * <p>Required by {@code PushProjectIntoTableSourceScanRule}.
     */
    @Test
    public void testProjectionReturnsDifferentSource() {
        TableSource<?> source =
                createTableSource(
                        TableSchema.builder()
                                .field("f0", DataTypes.INT())
                                .field("f1", DataTypes.STRING())
                                .field("f2", DataTypes.BIGINT())
                                .build());
        assumeThat(source, instanceOf(ProjectableTableSource.class));

        ProjectableTableSource<?> projectableTableSource = (ProjectableTableSource<?>) source;

        TableSource<?> newTableSource = projectableTableSource.projectFields(new int[] {0, 2});
        assertThat(newTableSource.explainSource(), not(equalTo(source.explainSource())));
        assertThat(newTableSource.getTableSchema(), equalTo(source.getTableSchema()));
    }
}

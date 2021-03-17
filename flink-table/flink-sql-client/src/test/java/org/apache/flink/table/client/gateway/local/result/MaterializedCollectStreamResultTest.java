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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.cli.utils.TestTableResult;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/** Tests for {@link MaterializedCollectStreamResult}. */
public class MaterializedCollectStreamResultTest {

    @Test
    public void testSnapshot() throws Exception {
        ResolvedSchema schema =
                ResolvedSchema.physical(
                        new String[] {"f0", "f1"},
                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT()});

        try (TestMaterializedCollectStreamResult result =
                new TestMaterializedCollectStreamResult(
                        new TestTableResult(ResultKind.SUCCESS_WITH_CONTENT, schema),
                        Integer.MAX_VALUE)) {
            result.isRetrieving = true;

            result.processRecord(Row.ofKind(RowKind.INSERT, "A", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "B", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "A", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "C", 2));

            assertEquals(TypedResult.payload(4), result.snapshot(1));

            assertEquals(Collections.singletonList(Row.of("A", 1)), result.retrievePage(1));
            assertEquals(Collections.singletonList(Row.of("B", 1)), result.retrievePage(2));
            assertEquals(Collections.singletonList(Row.of("A", 1)), result.retrievePage(3));
            assertEquals(Collections.singletonList(Row.of("C", 2)), result.retrievePage(4));

            result.processRecord(Row.ofKind(RowKind.UPDATE_BEFORE, "A", 1));

            assertEquals(TypedResult.payload(3), result.snapshot(1));

            assertEquals(Collections.singletonList(Row.of("A", 1)), result.retrievePage(1));
            assertEquals(Collections.singletonList(Row.of("B", 1)), result.retrievePage(2));
            assertEquals(Collections.singletonList(Row.of("C", 2)), result.retrievePage(3));

            result.processRecord(Row.ofKind(RowKind.UPDATE_BEFORE, "C", 2));
            result.processRecord(Row.ofKind(RowKind.UPDATE_BEFORE, "A", 1));
            result.processRecord(Row.ofKind(RowKind.UPDATE_AFTER, "D", 1));

            assertEquals(TypedResult.payload(2), result.snapshot(1));

            assertEquals(Collections.singletonList(Row.of("B", 1)), result.retrievePage(1));
            assertEquals(Collections.singletonList(Row.of("D", 1)), result.retrievePage(2));
        }
    }

    @Test
    public void testLimitedSnapshot() throws Exception {
        ResolvedSchema schema =
                ResolvedSchema.physical(
                        new String[] {"f0", "f1"},
                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT()});

        // limit the materialized table to 2 rows
        // with 3 rows overcommitment
        try (TestMaterializedCollectStreamResult result =
                new TestMaterializedCollectStreamResult(
                        new TestTableResult(ResultKind.SUCCESS_WITH_CONTENT, schema), 2, 3)) {

            result.isRetrieving = true;

            result.processRecord(Row.ofKind(RowKind.INSERT, "D", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "A", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "B", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "A", 1));

            assertEquals(
                    Arrays.asList(
                            null,
                            null,
                            Row.ofKind(RowKind.INSERT, "B", 1),
                            Row.ofKind(RowKind.INSERT, "A", 1)), // two over-committed rows
                    result.getMaterializedTable());

            assertEquals(TypedResult.payload(2), result.snapshot(1));

            assertEquals(
                    Collections.singletonList(Row.ofKind(RowKind.INSERT, "B", 1)),
                    result.retrievePage(1));
            assertEquals(
                    Collections.singletonList(Row.ofKind(RowKind.INSERT, "A", 1)),
                    result.retrievePage(2));

            result.processRecord(Row.ofKind(RowKind.INSERT, "C", 1));

            assertEquals(
                    Arrays.asList(
                            Row.ofKind(RowKind.INSERT, "A", 1),
                            Row.ofKind(RowKind.INSERT, "C", 1)), // limit clean up has taken place
                    result.getMaterializedTable());

            result.processRecord(Row.ofKind(RowKind.DELETE, "A", 1));

            assertEquals(
                    Collections.singletonList(
                            Row.ofKind(RowKind.INSERT, "C", 1)), // regular clean up has taken place
                    result.getMaterializedTable());
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    private static class TestMaterializedCollectStreamResult extends MaterializedCollectStreamResult
            implements AutoCloseable {

        public boolean isRetrieving;

        public TestMaterializedCollectStreamResult(
                TableResult tableResult, int maxRowCount, int overcommitThreshold) {
            super(tableResult, maxRowCount, overcommitThreshold);
        }

        public TestMaterializedCollectStreamResult(TableResult tableResult, int maxRowCount) {
            super(tableResult, maxRowCount);
        }

        @Override
        protected boolean isRetrieving() {
            return isRetrieving;
        }
    }
}

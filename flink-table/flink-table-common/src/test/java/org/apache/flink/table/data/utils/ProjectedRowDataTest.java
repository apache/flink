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

package org.apache.flink.table.data.utils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.test.RowDataAssert;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.test.TableAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProjectedRowData}. */
class ProjectedRowDataTest {

    @Test
    void testProjectedRows() {
        final RowData initialRow = GenericRowData.of(0L, 1L, 2L, 3L, 4L);
        final ProjectedRowData projectedRowData =
                ProjectedRowData.from(
                        new int[][] {new int[] {2}, new int[] {0}, new int[] {1}, new int[] {4}});
        final RowDataAssert rowAssert = assertThat(projectedRowData);
        projectedRowData.replaceRow(initialRow);

        rowAssert.hasKind(RowKind.INSERT).hasArity(4);
        rowAssert.getLong(0).isEqualTo(2);
        rowAssert.getLong(1).isEqualTo(0);
        rowAssert.getLong(2).isEqualTo(1);
        rowAssert.getLong(3).isEqualTo(4);

        projectedRowData.replaceRow(GenericRowData.of(5L, 6L, 7L, 8L, 9L, 10L));
        rowAssert.hasKind(RowKind.INSERT).hasArity(4);
        rowAssert.getLong(0).isEqualTo(7);
        rowAssert.getLong(1).isEqualTo(5);
        rowAssert.getLong(2).isEqualTo(6);
        rowAssert.getLong(3).isEqualTo(9);
    }

    @Test
    void testProjectedRowsDoesntSupportNestedProjections() {
        assertThatThrownBy(
                        () ->
                                ProjectedRowData.from(
                                        new int[][] {
                                            new int[] {2},
                                            new int[] {0, 1},
                                            new int[] {1},
                                            new int[] {4}
                                        }))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

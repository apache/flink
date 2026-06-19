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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ColumnReferenceFinder}. */
class ColumnReferenceFinderTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());
    private ResolvedSchema resolvedSchema;

    @BeforeEach
    void beforeEach() {
        resolvedSchema =
                util.testingTableEnv()
                        .getCatalogManager()
                        .getSchemaResolver()
                        .resolve(
                                Schema.newBuilder()
                                        .columnByExpression("a", "b || '_001'")
                                        .column("b", DataTypes.STRING())
                                        .columnByExpression("c", "d * e + 2")
                                        .column("d", DataTypes.DOUBLE())
                                        .columnByMetadata("e", DataTypes.INT(), null, true)
                                        .column(
                                                "tuple",
                                                DataTypes.ROW(
                                                        DataTypes.TIMESTAMP(3), DataTypes.INT()))
                                        .column("g", DataTypes.TIMESTAMP(3))
                                        .columnByExpression("ts", "tuple.f0")
                                        .watermark("ts", "g - interval '5' day")
                                        .build());
    }

    @Test
    void testFindReferencedColumn() {
        assertThat(ColumnReferenceFinder.findReferencedColumn("b", resolvedSchema))
                .isEqualTo(Collections.emptySet());

        assertThat(ColumnReferenceFinder.findReferencedColumn("a", resolvedSchema))
                .containsExactlyInAnyOrder("b");

        assertThat(ColumnReferenceFinder.findReferencedColumn("c", resolvedSchema))
                .containsExactlyInAnyOrder("d", "e");

        assertThat(ColumnReferenceFinder.findReferencedColumn("ts", resolvedSchema))
                .containsExactlyInAnyOrder("tuple");

        assertThat(ColumnReferenceFinder.findWatermarkReferencedColumn(resolvedSchema))
                .containsExactlyInAnyOrder("ts", "g");
    }
}

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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.test.WithTableEnvironment;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for Catalog constraints. */
@Execution(ExecutionMode.CONCURRENT)
public class CatalogConstraintTest {

    @Test
    @WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH)
    public void testWithPrimaryKey(TableEnvironment tEnv) {
        tEnv.createTable(
                "T1",
                TableDescriptor.forConnector("filesystem")
                        .schema(
                                Schema.newBuilder()
                                        .column("a", STRING())
                                        .column("b", BIGINT().notNull())
                                        .column("c", INT())
                                        .primaryKey("b")
                                        .build())
                        .option("path", "/path/to/csv")
                        .format("testcsv")
                        .build());

        RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getUniqueKeys(t1)).isEqualTo(ImmutableSet.of(ImmutableBitSet.of(1)));
    }

    @Test
    @WithTableEnvironment(executionMode = RuntimeExecutionMode.BATCH)
    public void testWithoutPrimaryKey(TableEnvironment tEnv) {
        tEnv.createTable(
                "T1",
                TableDescriptor.forConnector("filesystem")
                        .schema(
                                Schema.newBuilder()
                                        .column("a", BIGINT())
                                        .column("b", STRING())
                                        .column("c", INT())
                                        .build())
                        .option("path", "/path/to/csv")
                        .format("testcsv")
                        .build());

        RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getUniqueKeys(t1)).isEqualTo(ImmutableSet.of());
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */

package org.apache.flink.table.planner.runtime.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Test for parallelism setting when translating ExecNode to Transformation. */
public class ParallelismSettingTest extends TableTestBase {

    private BatchTableTestUtil util;

    @Before
    public void before() {
        util = batchTestUtil(TableConfig.getDefault());

        util.getStreamEnv().getConfig().setDynamicGraph(true);
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a BIGINT,\n"
                                + "  b BIGINT,\n"
                                + "  c VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'format' = 'testcsv',\n"
                                + "  'path' = '/tmp')");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  b bigint\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false',\n"
                                + "  'table-sink-class' = 'DEFAULT')");
    }

    @Test
    public void testParallelismSettingAfterSingletonShuffleRemove() {
        List<Operation> operations =
                util.getPlanner()
                        .getParser()
                        .parse(
                                "INSERT INTO MySink SELECT MAX(b) FROM (SELECT SUM(b) AS b FROM MyTable)");
        // the exec plan:
        // Sink(table=[default_catalog.default_database.MySink], fields=[EXPR$0])
        // +- HashAggregate(isMerge=[false], select=[MAX(b) AS EXPR$0])
        //   +- HashAggregate(isMerge=[true], select=[Final_SUM(sum$0) AS b])
        //      +- Exchange(distribution=[single])
        //         +- LocalHashAggregate(select=[Partial_SUM(b) AS sum$0])
        //            +- TableSourceScan(table=[[MyTable, project=[b])

        List<Transformation<?>> transformations =
                util.getPlanner()
                        .translate(
                                Collections.singletonList((ModifyOperation) (operations.get(0))));
        assertEquals(1, transformations.size());
        Transformation<?> sink = transformations.get(0);
        Transformation<?> topAgg = sink.getInputs().get(0);
        assertEquals(1, topAgg.getParallelism());
        assertEquals(1, topAgg.getMaxParallelism());
        Transformation<?> bottomAgg = topAgg.getInputs().get(0);
        assertEquals(1, bottomAgg.getParallelism());
        assertEquals(1, bottomAgg.getMaxParallelism());
    }
}

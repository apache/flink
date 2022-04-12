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

package org.apache.flink.table.planner.plan.nodes.exec.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Test;

import java.util.Optional;

/** Tests for verifying name and description of batch sql operator. */
public class BatchOperatorNameTest extends OperatorNameTestBase {

    @Override
    protected TableTestUtil getTableTestUtil() {
        return batchTestUtil(TableConfig.getDefault());
    }

    @Test
    public void testBoundedStreamScan() {
        final DataStream<Integer> dataStream = util.getStreamEnv().fromElements(1, 2, 3, 4, 5);
        TableTestUtil.createTemporaryView(
                tEnv,
                "MyTable",
                dataStream,
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()));
        verifyQuery("SELECT * FROM MyTable");
    }

    /** Verify Expand, HashAggregate. */
    @Test
    public void testHashAggregate() {
        createTestSource();
        verifyQuery("SELECT a, " + "count(distinct b) as b " + "FROM MyTable GROUP BY a");
    }

    /** Verify Sort, SortAggregate. */
    @Test
    public void testSortAggregate() {
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg");
        createTestSource();
        verifyQuery("SELECT a, " + "count(distinct b) as b " + "FROM MyTable GROUP BY a");
    }

    /** Verify SortWindowAggregate. */
    @Test
    public void testSortWindowAggregate() {
        createSourceWithTimeAttribute();
        verifyQuery(
                "SELECT\n"
                        + "  b,\n"
                        + "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE) as window_end,\n"
                        + "  FIRST_VALUE(a)\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b, TUMBLE(rowtime, INTERVAL '15' MINUTE)");
    }

    /** Verify HashJoin. */
    @Test
    public void testHashJoin() {
        testJoinInternal();
    }

    /** Verify NestedLoopJoin. */
    @Test
    public void testNestedLoopJoin() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "HashJoin, SortMergeJoin");
        testJoinInternal();
    }

    /** Verify SortMergeJoin. */
    @Test
    public void testSortMergeJoin() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "HashJoin, NestedLoopJoin");
        testJoinInternal();
    }

    /** Verify MultiInput. */
    @Test
    public void testMultiInput() {
        createTestSource("A");
        createTestSource("B");
        createTestSource("C");
        verifyQuery("SELECT * FROM A, B, C where A.a = B.a and A.a = C.a");
    }

    /** Verify Limit. */
    @Test
    public void testLimit() {
        createTestSource();
        verifyQuery("select * from MyTable limit 10");
    }

    /** Verify SortLimit. */
    @Test
    public void testSortLimit() {
        createTestSource();
        verifyQuery("select * from MyTable order by a limit 10");
    }
}

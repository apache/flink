/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraphGenerator;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.util.FileUtils;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/** Tests for {@link MultipleInputNodeCreationProcessor}. */
public class MultipleInputNodeCreationProcessorTest extends TableTestBase {

    private final BatchTableTestUtil batchUtil = batchTestUtil(new TableConfig());
    private final StreamTableTestUtil streamUtil = streamTestUtil(new TableConfig());

    @Test
    public void testIsChainableDataStreamSource() {
        createChainableStream(batchUtil);
        assertChainableSource("chainableStream", batchUtil, true);
        createChainableStream(streamUtil);
        assertChainableSource("chainableStream", streamUtil, true);
    }

    @Test
    public void testNonChainableDataStreamSource() {
        createNonChainableStream(batchUtil);
        assertChainableSource("nonChainableStream", batchUtil, false);
        createNonChainableStream(streamUtil);
        assertChainableSource("nonChainableStream", streamUtil, false);
    }

    @Test
    public void testIsChainableTableSource() throws IOException {
        createTestFileSource(batchUtil.tableEnv(), "fileSource1", "Source");
        assertChainableSource("fileSource1", batchUtil, true);
        createTestFileSource(streamUtil.tableEnv(), "fileSource1", "Source");
        assertChainableSource("fileSource1", streamUtil, true);

        createTestFileSource(batchUtil.tableEnv(), "fileSource2", "DataStream");
        assertChainableSource("fileSource2", batchUtil, true);
        createTestFileSource(streamUtil.tableEnv(), "fileSource2", "DataStream");
        assertChainableSource("fileSource2", streamUtil, true);
    }

    @Test
    public void testNonChainableTableSource() throws IOException {
        createTestValueSource(batchUtil.tableEnv(), "valueSource1", "DataStream");
        assertChainableSource("valueSource1", batchUtil, false);
        createTestValueSource(streamUtil.tableEnv(), "valueSource1", "DataStream");
        assertChainableSource("valueSource1", streamUtil, false);

        createTestValueSource(batchUtil.tableEnv(), "valueSource2", "SourceFunction");
        assertChainableSource("valueSource2", batchUtil, false);
        createTestValueSource(streamUtil.tableEnv(), "valueSource2", "SourceFunction");
        assertChainableSource("valueSource2", streamUtil, false);

        createTestValueSource(batchUtil.tableEnv(), "valueSource3", "InputFormat");
        assertChainableSource("valueSource3", batchUtil, false);
        createTestValueSource(streamUtil.tableEnv(), "valueSource3", "InputFormat");
        assertChainableSource("valueSource3", streamUtil, false);
    }

    private void assertChainableSource(String name, TableTestUtil util, boolean expected) {
        String sql = "SELECT * FROM " + name;
        Table table = util.tableEnv().sqlQuery(sql);
        RelNode relNode = TableTestUtil.toRelNode(table);
        FlinkPhysicalRel optimizedRel = (FlinkPhysicalRel) util.getPlanner().optimize(relNode);
        ExecNodeGraphGenerator generator = new ExecNodeGraphGenerator();
        ExecNodeGraph execGraph = generator.generate(Collections.singletonList(optimizedRel));
        ExecNode<?> execNode = execGraph.getRootNodes().get(0);
        while (!execNode.getInputEdges().isEmpty()) {
            execNode = execNode.getInputEdges().get(0).getSource();
        }
        DAGProcessContext context = new DAGProcessContext(util.getPlanner());
        Assert.assertEquals(
                expected, MultipleInputNodeCreationProcessor.isChainableSource(execNode, context));
    }

    private void createChainableStream(TableTestUtil util) {
        DataStreamSource<Integer> dataStream =
                util.getStreamEnv()
                        .fromSource(
                                new MockSource(Boundedness.BOUNDED, 1),
                                WatermarkStrategy.noWatermarks(),
                                "chainableStream");
        TableTestUtil.createTemporaryView(
                util.tableEnv(),
                "chainableStream",
                dataStream,
                scala.Option.apply(new Expression[] {ApiExpressionUtils.unresolvedRef("a")}),
                scala.Option.empty(),
                scala.Option.empty());
    }

    private void createNonChainableStream(TableTestUtil util) {
        DataStreamSource<Integer> dataStream = util.getStreamEnv().fromElements(1, 2, 3);
        TableTestUtil.createTemporaryView(
                util.tableEnv(),
                "nonChainableStream",
                dataStream,
                scala.Option.apply(new Expression[] {ApiExpressionUtils.unresolvedRef("a")}),
                scala.Option.empty(),
                scala.Option.empty());
    }

    private void createTestFileSource(TableEnvironment tEnv, String name, String runtimeSource)
            throws IOException {
        File file = tempFolder().newFile();
        file.delete();
        file.createNewFile();
        FileUtils.writeFileUtf8(file, "1\n2\n3\n");
        tEnv.executeSql(
                "CREATE TABLE "
                        + name
                        + "(\n"
                        + "  a STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'test-file',\n"
                        + "  'path' = '"
                        + file.toURI()
                        + "',\n"
                        + "  'runtime-source' = '"
                        + runtimeSource
                        + "'\n"
                        + ")");
    }

    private void createTestValueSource(TableEnvironment tEnv, String name, String runtimeSource) {
        tEnv.executeSql(
                "CREATE TABLE "
                        + name
                        + "(\n"
                        + "  a STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true',\n"
                        + "  'runtime-source' = '"
                        + runtimeSource
                        + "'\n"
                        + ")");
    }
}

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

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Tests for {@link ResetTransformationProcessor}. */
public class ResetTransformationProcessorTest extends TableTestBase {

    private BatchTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void begin() {
        util = batchTestUtil(TableConfig.getDefault());
        tEnv = util.tableEnv();

        tEnv.executeSql(
                "CREATE TABLE Source1 (\n"
                        + "  a BIGINT,\n"
                        + "  b BIGINT,\n"
                        + "  c VARCHAR,\n"
                        + "  d BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE Source2 (\n"
                        + "  a BIGINT,\n"
                        + "  b BIGINT,\n"
                        + "  c VARCHAR,\n"
                        + "  d BIGINT\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")");
    }

    @Test
    public void testResetTransformation() {
        String query = "SELECT * FROM Source1 WHERE a > 100";
        ExecNodeGraph execNodeGraph = TableTestUtil.toExecNodeGraph(tEnv, query);
        execNodeGraph.getRootNodes().forEach(node -> node.translateToPlan(util.getPlanner()));
        assertAllTransformationsIsNotNull(execNodeGraph);

        ResetTransformationProcessor resetTransformationProcessor =
                new ResetTransformationProcessor();
        resetTransformationProcessor.process(
                execNodeGraph, new ProcessorContext(util.getPlanner()));
        assertAllTransformationsIsNull(execNodeGraph);
    }

    @Test
    public void testResetTransformationWithExecMultipleInputInExecGraph() {
        String query =
                "SELECT Source1.a FROM Source1, Source2 "
                        + "WHERE Source1.a = Source2.a AND Source2.a = (SELECT Source2.a FROM Source2 WHERE b > 100)";
        ExecNodeGraph execNodeGraph = TableTestUtil.toExecNodeGraph(tEnv, query);

        MultipleInputNodeCreationProcessor multipleInputNodeCreationProcessor =
                new MultipleInputNodeCreationProcessor(false);
        multipleInputNodeCreationProcessor.process(
                execNodeGraph, new ProcessorContext(util.getPlanner()));
        execNodeGraph.getRootNodes().forEach(node -> node.translateToPlan(util.getPlanner()));
        assertAllTransformationsIsNotNull(execNodeGraph);

        // If execNodeGraph include batchExecMultipleInput node. The ResetTransformationProcessor
        // need to both set the transformation in each execNode and the transformation in
        // batchExecMultipleInput.rootNode to null.
        ResetTransformationProcessor resetTransformationProcessor =
                new ResetTransformationProcessor();
        resetTransformationProcessor.process(
                execNodeGraph, new ProcessorContext(util.getPlanner()));
        assertAllTransformationsIsNull(execNodeGraph);
    }

    private void assertAllTransformationsIsNotNull(ExecNodeGraph execNodeGraph) {
        AbstractExecNodeExactlyOnceVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        assertNotNull(((ExecNodeBase<?>) node).getTransformation());
                        visitInputs(node);
                        if (node instanceof BatchExecMultipleInput) {
                            ExecNode<?> rootNode = ((BatchExecMultipleInput) node).getRootNode();
                            assertNotNull(((ExecNodeBase<?>) node).getTransformation());
                            visitInputs(rootNode);
                        }
                    }
                };
        execNodeGraph.getRootNodes().forEach(r -> r.accept(visitor));
    }

    private void assertAllTransformationsIsNull(ExecNodeGraph execNodeGraph) {
        AbstractExecNodeExactlyOnceVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        assertNull(((ExecNodeBase<?>) node).getTransformation());
                        visitInputs(node);
                        if (node instanceof BatchExecMultipleInput) {
                            ExecNode<?> rootNode = ((BatchExecMultipleInput) node).getRootNode();
                            assertNull(((ExecNodeBase<?>) node).getTransformation());
                            visitInputs(rootNode);
                        }
                    }
                };
        execNodeGraph.getRootNodes().forEach(r -> r.accept(visitor));
    }
}

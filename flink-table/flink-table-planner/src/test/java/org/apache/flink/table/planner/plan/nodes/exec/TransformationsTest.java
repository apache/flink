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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.WithBoundedness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.TransformationScanProvider;
import org.apache.flink.table.planner.utils.JavaBatchTableTestUtil;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Various tests to check {@link Transformation}s that have been generated from {@link ExecNode}s.
 */
public class TransformationsTest extends TableTestBase {

    @Test
    public void testLegacyBatchSource() {
        final JavaBatchTableTestUtil util = javaBatchTestUtil();
        final StreamTableEnvironment env = util.tableEnv();

        final Table table =
                env.from(
                        TableDescriptor.forConnector("values")
                                .option("bounded", "true")
                                .schema(dummySchema())
                                .build());

        final LegacySourceTransformation<?> sourceTransform =
                toLegacySourceTransformation(env, table);

        assertBoundedness(Boundedness.BOUNDED, sourceTransform);
        assertFalse(sourceTransform.getOperator().emitsProgressiveWatermarks());
    }

    @Test
    public void testLegacyStreamSource() {
        final JavaStreamTableTestUtil util = javaStreamTestUtil();
        final StreamTableEnvironment env = util.tableEnv();

        final Table table =
                env.from(
                        TableDescriptor.forConnector("values")
                                .option("bounded", "false")
                                .schema(dummySchema())
                                .build());

        final LegacySourceTransformation<?> sourceTransform =
                toLegacySourceTransformation(env, table);

        assertBoundedness(Boundedness.CONTINUOUS_UNBOUNDED, sourceTransform);
        assertTrue(sourceTransform.getOperator().emitsProgressiveWatermarks());
    }

    @Test
    public void testStreamTransformationScanProvider() {
        final JavaStreamTableTestUtil util = javaStreamTestUtil();
        final StreamTableEnvironment env = util.tableEnv();

        final Table table =
                env.from(
                        TableDescriptor.forConnector("values")
                                .option("bounded", "false")
                                .schema(dummySchema())
                                .build());

        final Transformation<RowData> transformation =
                env.toChangelogStream(table)
                        .<RowData>map(r -> new GenericRowData(0))
                        .getTransformation();

        assertFalse(TransformationScanProvider.of(transformation).isBounded());
    }

    @Test
    public void testBatchTransformationScanProvider() {
        final JavaBatchTableTestUtil util = javaBatchTestUtil();
        final StreamTableEnvironment env = util.tableEnv();

        final Table table =
                env.from(
                        TableDescriptor.forConnector("values")
                                .option("bounded", "true")
                                .schema(dummySchema())
                                .build());

        final Transformation<RowData> transformation =
                env.toChangelogStream(table)
                        .<RowData>map(r -> new GenericRowData(0))
                        .getTransformation();

        assertTrue(TransformationScanProvider.of(transformation).isBounded());
    }

    @Test
    public void testLegacyBatchValues() {
        final JavaBatchTableTestUtil util = javaBatchTestUtil();
        final StreamTableEnvironment env = util.tableEnv();

        final Table table = env.fromValues(1, 2, 3);

        final LegacySourceTransformation<?> sourceTransform =
                toLegacySourceTransformation(env, table);

        assertBoundedness(Boundedness.BOUNDED, sourceTransform);
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static LegacySourceTransformation<?> toLegacySourceTransformation(
            StreamTableEnvironment env, Table table) {
        Transformation<?> transform = env.toChangelogStream(table).getTransformation();
        while (transform.getInputs().size() == 1) {
            transform = transform.getInputs().get(0);
        }
        assertTrue(transform instanceof LegacySourceTransformation);
        return (LegacySourceTransformation<?>) transform;
    }

    private static void assertBoundedness(Boundedness boundedness, Transformation<?> transform) {
        assertTrue(transform instanceof WithBoundedness);
        assertEquals(boundedness, ((WithBoundedness) transform).getBoundedness());
    }

    private static Schema dummySchema() {
        return Schema.newBuilder().column("i", DataTypes.INT()).build();
    }
}

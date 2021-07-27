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

import org.apache.flink.api.common.ShuffleMode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.WithBoundedness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Various tests to check {@link Transformation}s that have been generated from {@link ExecNode}s.
 */
public class TransformationsTest extends TableTestBase {

    @Test
    public void testLegacyBatchSource() {
        final JavaStreamTableTestUtil util = javaStreamTestUtil();
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

    @Ignore // will be enabled in FLINK-20897
    @Test
    public void testUnsupportedShuffleMode() {
        final JavaStreamTableTestUtil util = javaStreamTestUtil();
        final StreamExecutionEnvironment env = util.env();
        final StreamTableEnvironment tableEnv = util.tableEnv();

        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.SHUFFLE_MODE, ShuffleMode.ALL_EXCHANGES_BLOCKING);
        env.configure(configuration);

        final IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> tableEnv.executeSql("SELECT COUNT(*) FROM (VALUES 1, 2, 3)"));
        assertEquals(
                exception.getMessage(),
                "Unsupported shuffle mode 'ALL_EXCHANGES_BLOCKING' in STREAMING runtime mode.");
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static LegacySourceTransformation<?> toLegacySourceTransformation(
            StreamTableEnvironment env, Table table) {
        final Transformation<?> transform = env.toChangelogStream(table).getTransformation();
        assertFalse(transform.getInputs().isEmpty());

        final Transformation<?> sourceTransform = transform.getInputs().get(0);
        assertTrue(sourceTransform instanceof LegacySourceTransformation);
        return (LegacySourceTransformation<?>) sourceTransform;
    }

    private static void assertBoundedness(Boundedness boundedness, Transformation<?> transform) {
        assertTrue(transform instanceof WithBoundedness);
        assertEquals(boundedness, ((WithBoundedness) transform).getBoundedness());
    }

    private static Schema dummySchema() {
        return Schema.newBuilder().column("i", DataTypes.INT()).build();
    }
}

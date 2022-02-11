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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.runtime.groupwindow.WindowReference;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link LogicalWindow} serialization and deserialization. */
@RunWith(Parameterized.class)
public class LogicalWindowSerdeTest {

    @Parameterized.Parameter public LogicalWindow window;

    @Parameterized.Parameters(name = "{0}")
    public static List<LogicalWindow> testData() {
        return Arrays.asList(
                new TumblingGroupWindow(
                        new WindowReference(
                                "timeWindow", new TimestampType(false, TimestampKind.ROWTIME, 3)),
                        new FieldReferenceExpression(
                                "rowTime",
                                new AtomicDataType(
                                        new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                1,
                                2),
                        new ValueLiteralExpression(Duration.ofMinutes(10))),
                new TumblingGroupWindow(
                        new WindowReference("countWindow", new BigIntType()),
                        new FieldReferenceExpression(
                                "rowTime",
                                new AtomicDataType(
                                        new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                1,
                                2),
                        new ValueLiteralExpression(10L)),
                new SlidingGroupWindow(
                        new WindowReference(
                                "timeWindow", new TimestampType(false, TimestampKind.ROWTIME, 3)),
                        new FieldReferenceExpression(
                                "rowTime",
                                new AtomicDataType(
                                        new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                1,
                                2),
                        new ValueLiteralExpression(Duration.ofSeconds(10)),
                        new ValueLiteralExpression(Duration.ofSeconds(5))),
                new SlidingGroupWindow(
                        new WindowReference("countWindow", new BigIntType()),
                        new FieldReferenceExpression(
                                "rowTime",
                                new AtomicDataType(
                                        new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                1,
                                2),
                        new ValueLiteralExpression(10L),
                        new ValueLiteralExpression(5L)),
                new SessionGroupWindow(
                        new WindowReference(
                                "timeWindow", new TimestampType(false, TimestampKind.ROWTIME, 3)),
                        new FieldReferenceExpression(
                                "rowTime",
                                new AtomicDataType(
                                        new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                1,
                                2),
                        new ValueLiteralExpression(Duration.ofDays(10))));
    }

    @Test
    public void testLogicalWindowSerde() throws IOException {
        SerdeContext serdeCtx =
                new SerdeContext(
                        null,
                        new FlinkContextImpl(
                                false,
                                TableConfig.getDefault(),
                                new ModuleManager(),
                                null,
                                CatalogManagerMocks.createEmptyCatalogManager(),
                                null),
                        Thread.currentThread().getContextClassLoader(),
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());

        ObjectReader objectReader = JsonSerdeUtil.createObjectReader(serdeCtx);
        ObjectWriter objectWriter = JsonSerdeUtil.createObjectWriter(serdeCtx);

        assertEquals(
                objectReader.readValue(
                        objectWriter.writeValueAsString(window), LogicalWindow.class),
                window);
    }
}

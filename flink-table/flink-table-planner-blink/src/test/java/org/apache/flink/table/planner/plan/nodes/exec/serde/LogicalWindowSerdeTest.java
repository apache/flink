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
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.PlannerWindowReference;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.rex.RexNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
        List<LogicalWindow> types =
                Arrays.asList(
                        new TumblingGroupWindow(
                                new PlannerWindowReference(
                                        "timeWindow",
                                        new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                new FieldReferenceExpression(
                                        "rowTime",
                                        new AtomicDataType(
                                                new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                        1,
                                        2),
                                new ValueLiteralExpression(Duration.ofSeconds(10))),
                        new TumblingGroupWindow(
                                new PlannerWindowReference("countWindow", new BigIntType()),
                                new FieldReferenceExpression(
                                        "rowTime",
                                        new AtomicDataType(
                                                new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                        1,
                                        2),
                                new ValueLiteralExpression(10L)),
                        new SlidingGroupWindow(
                                new PlannerWindowReference(
                                        "timeWindow",
                                        new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                new FieldReferenceExpression(
                                        "rowTime",
                                        new AtomicDataType(
                                                new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                        1,
                                        2),
                                new ValueLiteralExpression(Duration.ofSeconds(10)),
                                new ValueLiteralExpression(Duration.ofSeconds(5))),
                        new SlidingGroupWindow(
                                new PlannerWindowReference("countWindow", new BigIntType()),
                                new FieldReferenceExpression(
                                        "rowTime",
                                        new AtomicDataType(
                                                new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                        1,
                                        2),
                                new ValueLiteralExpression(10L),
                                new ValueLiteralExpression(5L)),
                        new SessionGroupWindow(
                                new PlannerWindowReference(
                                        "timeWindow",
                                        new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                new FieldReferenceExpression(
                                        "rowTime",
                                        new AtomicDataType(
                                                new TimestampType(false, TimestampKind.ROWTIME, 3)),
                                        1,
                                        2),
                                new ValueLiteralExpression(Duration.ofSeconds(10))));
        return types;
    }

    @Test
    public void testLogicalWindowSerde() throws JsonProcessingException {
        SerdeContext serdeCtx =
                new SerdeContext(
                        new FlinkContextImpl(TableConfig.getDefault(), null, null, null),
                        Thread.currentThread().getContextClassLoader(),
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        SimpleModule module = new SimpleModule();

        module.addSerializer(new DurationJsonSerializer());
        module.addDeserializer(Duration.class, new DurationJsonDeserializer());
        module.addSerializer(new RexNodeJsonSerializer());
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        module.addSerializer(new LogicalTypeJsonSerializer());
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());
        module.addSerializer(new LogicalWindowJsonSerializer());
        module.addDeserializer(LogicalWindow.class, new LogicalWindowJsonDeserializer());
        mapper.registerModule(module);

        assertEquals(
                mapper.readValue(mapper.writeValueAsString(window), LogicalWindow.class).toString(),
                window.toString());
    }
}

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
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test json serialization/deserialization for {@link RexWindowBound}. */
public class RexWindowBoundSerdeTest {

    @Test
    public void testSerde() throws JsonProcessingException {
        SerdeContext serdeCtx =
                new SerdeContext(
                        new FlinkContextImpl(false, TableConfig.getDefault(), null, null, null),
                        Thread.currentThread().getContextClassLoader(),
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        SimpleModule module = new SimpleModule();
        module.addSerializer(new RexWindowBoundJsonSerializer());
        module.addDeserializer(RexWindowBound.class, new RexWindowBoundJsonDeserializer());
        module.addSerializer(new RexNodeJsonSerializer());
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        module.addSerializer(new RelDataTypeJsonSerializer());
        module.addDeserializer(RelDataType.class, new RelDataTypeJsonDeserializer());
        mapper.registerModule(module);

        assertEquals(
                RexWindowBounds.CURRENT_ROW,
                mapper.readValue(
                        mapper.writeValueAsString(RexWindowBounds.CURRENT_ROW),
                        RexWindowBound.class));

        assertEquals(
                RexWindowBounds.UNBOUNDED_FOLLOWING,
                mapper.readValue(
                        mapper.writeValueAsString(RexWindowBounds.UNBOUNDED_FOLLOWING),
                        RexWindowBound.class));

        assertEquals(
                RexWindowBounds.UNBOUNDED_PRECEDING,
                mapper.readValue(
                        mapper.writeValueAsString(RexWindowBounds.UNBOUNDED_PRECEDING),
                        RexWindowBound.class));

        RexBuilder builder = new RexBuilder(FlinkTypeFactory.INSTANCE());
        RexWindowBound windowBound = RexWindowBounds.following(builder.makeLiteral("test"));
        assertEquals(
                windowBound,
                mapper.readValue(mapper.writeValueAsString(windowBound), RexWindowBound.class));

        windowBound = RexWindowBounds.preceding(builder.makeLiteral("test"));
        assertEquals(
                windowBound,
                mapper.readValue(mapper.writeValueAsString(windowBound), RexWindowBound.class));
    }
}

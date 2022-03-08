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
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Test json serialization/deserialization for {@link RexWindowBound}. */
public class RexWindowBoundSerdeTest {

    @Test
    public void testSerde() throws IOException {
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
                RexWindowBounds.CURRENT_ROW,
                objectReader.readValue(
                        objectWriter.writeValueAsString(RexWindowBounds.CURRENT_ROW),
                        RexWindowBound.class));

        assertEquals(
                RexWindowBounds.UNBOUNDED_FOLLOWING,
                objectReader.readValue(
                        objectWriter.writeValueAsString(RexWindowBounds.UNBOUNDED_FOLLOWING),
                        RexWindowBound.class));

        assertEquals(
                RexWindowBounds.UNBOUNDED_PRECEDING,
                objectReader.readValue(
                        objectWriter.writeValueAsString(RexWindowBounds.UNBOUNDED_PRECEDING),
                        RexWindowBound.class));

        RexBuilder builder = new RexBuilder(FlinkTypeFactory.INSTANCE());
        RexWindowBound windowBound = RexWindowBounds.following(builder.makeLiteral("test"));
        assertEquals(
                windowBound,
                objectReader.readValue(
                        objectWriter.writeValueAsString(windowBound), RexWindowBound.class));

        windowBound = RexWindowBounds.preceding(builder.makeLiteral("test"));
        assertEquals(
                windowBound,
                objectReader.readValue(
                        objectWriter.writeValueAsString(windowBound), RexWindowBound.class));
    }
}

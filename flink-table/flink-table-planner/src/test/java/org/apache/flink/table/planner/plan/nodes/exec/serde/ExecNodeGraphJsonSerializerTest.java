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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExecNodeGraphJsonSerializer}. */
class ExecNodeGraphJsonSerializerTest {

    @Test
    void testSerializingUnsupportedNode() {
        final ObjectWriter objectWriter =
                JsonSerdeUtil.createObjectWriter(JsonSerdeTestUtil.configuredSerdeContext());
        assertThatThrownBy(
                        () ->
                                objectWriter.writeValueAsString(
                                        new ExecNodeGraph(
                                                FlinkVersion.v1_18,
                                                Collections.singletonList(new NoAnnotationNode()))))
                .hasMessageContaining(
                        "Can not serialize ExecNode with id: 10. Missing type, this is a bug, please file a ticket");
    }

    private static class NoAnnotationNode extends ExecNodeBase<RowData> {

        NoAnnotationNode() {
            super(
                    10,
                    ExecNodeContext.newContext(NoAnnotationNode.class),
                    new Configuration(),
                    Collections.emptyList(),
                    DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())).getLogicalType(),
                    "");
            setInputEdges(Collections.emptyList());
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }
}

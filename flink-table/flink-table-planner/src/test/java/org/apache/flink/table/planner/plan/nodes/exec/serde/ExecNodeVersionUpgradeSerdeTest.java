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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link org.apache.flink.table.planner.plan.nodes.exec.ExecNode} deserialization after
 * upgrading to a higher version from serialized plan which is serialized using the old version.
 */
public class ExecNodeVersionUpgradeSerdeTest {

    @Test
    public void testDeserializeOldVersionUsingNewVersion() throws IOException {
        ExecNodeMetadataUtil.addTestNode(DummyExecNode.class);
        String serializedUsingOldVersion =
                "{\"id\":1,\"type\":\"dummy-exec-node_1\",\"inputProperties\":[],\"outputType\":\"ROW<>\",\"description\":\"Dummy\"}";

        SerdeContext context = configuredSerdeContext();
        DummyExecNode deserializedUsingNewVersion =
                toObject(context, serializedUsingOldVersion, DummyExecNode.class);
        assertThat(toJson(context, deserializedUsingNewVersion))
                .isEqualTo(serializedUsingOldVersion);
    }

    @ExecNodeMetadata(
            name = "dummy-exec-node",
            version = 1,
            minPlanVersion = FlinkVersion.v1_15,
            minStateVersion = FlinkVersion.v1_15)
    @ExecNodeMetadata(
            name = "dummy-exec-node",
            version = 2,
            minPlanVersion = FlinkVersion.v1_18,
            minStateVersion = FlinkVersion.v1_15)
    private static class DummyExecNode extends ExecNodeBase<RowData> {

        private static final String FIELD_NAME_NEW_ADDED = "newProperty";

        /** DummyExecNode gets an additional property in Flink 1.18. */
        private final Integer newProperty;

        @JsonCreator
        protected DummyExecNode(
                @JsonProperty(FIELD_NAME_ID) int id,
                @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
                @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
                @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
                @JsonProperty(FIELD_NAME_OUTPUT_TYPE) LogicalType outputType,
                @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
                @JsonProperty(FIELD_NAME_NEW_ADDED) Integer newProperty) {
            super(id, context, persistedConfig, inputProperties, outputType, description);
            this.newProperty = newProperty;
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(
                PlannerBase planner, ExecNodeConfig config) {
            return null;
        }
    }
}

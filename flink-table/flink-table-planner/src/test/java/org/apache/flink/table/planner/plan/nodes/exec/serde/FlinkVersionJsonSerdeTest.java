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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link FlinkVersion} serialization and deserialization. */
@Execution(CONCURRENT)
final class FlinkVersionJsonSerdeTest {

    @ParameterizedTest
    @EnumSource(FlinkVersion.class)
    void testFlinkVersions(FlinkVersion flinkVersion) throws IOException {
        testJsonRoundTrip(flinkVersion, FlinkVersion.class);
    }

    @Test
    void testManualString() throws IOException {
        final SerdeContext ctx = configuredSerdeContext();

        final String flinkVersion = "1.15";

        assertThat(toJson(ctx, FlinkVersion.v1_15))
                .isEqualTo(JsonSerdeUtil.createObjectWriter(ctx).writeValueAsString(flinkVersion));
    }
}

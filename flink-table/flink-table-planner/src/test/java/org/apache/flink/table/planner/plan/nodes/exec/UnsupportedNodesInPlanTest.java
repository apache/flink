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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for deserialising invalid {@link org.apache.flink.table.api.CompiledPlan}. */
public class UnsupportedNodesInPlanTest extends TableTestBase {

    @Test
    public void testInvalidType() {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        assertThatThrownBy(
                        () ->
                                tEnv.loadPlan(
                                        PlanReference.fromResource(
                                                "/jsonplan/testInvalidTypeJsonPlan.json")))
                .hasRootCauseMessage("Unsupported exec node type: 'null_null'.");
    }
}

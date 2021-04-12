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

import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.serde.ExecNodeGraphJsonPlanGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The {@link ExecNodeGraph} representing the {@link ExecNode} topology. */
public class ExecNodeGraph {
    private final String flinkVersion;
    private final List<ExecNode<?>> rootNodes;

    public ExecNodeGraph(List<ExecNode<?>> rootNodes) {
        this(EnvironmentInformation.getVersion(), rootNodes);
    }

    public ExecNodeGraph(String flinkVersion, List<ExecNode<?>> rootNodes) {
        this.flinkVersion = checkNotNull(flinkVersion, "The flinkVersion should not be null.");
        this.rootNodes = checkNotNull(rootNodes, "The rootNodes should not be null.");
        checkArgument(!rootNodes.isEmpty(), "The rootNodes should not be empty.");
    }

    public List<ExecNode<?>> getRootNodes() {
        return rootNodes;
    }

    public String getFlinkVersion() {
        return flinkVersion;
    }

    public static String createJsonPlan(ExecNodeGraph execGraph, SerdeContext serdeCtx) {
        try {
            return ExecNodeGraphJsonPlanGenerator.generateJsonPlan(execGraph, serdeCtx);
        } catch (IOException e) {
            throw new TableException("Failed to create json plan.", e);
        }
    }

    public static ExecNodeGraph createExecNodeGraph(String jsonPlan, SerdeContext serdeCtx) {
        try {
            return ExecNodeGraphJsonPlanGenerator.generateExecNodeGraph(jsonPlan, serdeCtx);
        } catch (IOException e) {
            throw new TableException("Failed to create ExecNodeGraph.", e);
        }
    }
}

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

package org.apache.flink.table.planner.plan;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.CompiledPlanInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/** Implementation of {@link CompiledPlan} backed by an {@link ExecNodeGraph}. */
@Internal
public class ExecNodeGraphCompiledPlan implements CompiledPlanInternal {

    private final PlannerBase planner;
    private final String serializedPlan;
    private final ExecNodeGraph execNodeGraph;

    public ExecNodeGraphCompiledPlan(
            PlannerBase planner, String serializedPlan, ExecNodeGraph execNodeGraph) {
        this.planner = planner;
        this.serializedPlan = serializedPlan;
        this.execNodeGraph = execNodeGraph;
    }

    public ExecNodeGraph getExecNodeGraph() {
        return execNodeGraph;
    }

    @Override
    public String asJsonString() {
        return serializedPlan;
    }

    @Override
    public void writeToFile(File file, boolean ignoreIfExists) throws IOException {
        if (!ignoreIfExists && file.exists()) {
            throw new TableException(
                    "The plan file '"
                            + file
                            + "' already exists. "
                            + "If you want to recompile the plan, please manually remove the file.");
        }
        FileUtils.writeFileUtf8(file, serializedPlan);
    }

    @Override
    public String getFlinkVersion() {
        return this.execNodeGraph.getFlinkVersion();
    }

    @Override
    public String explain(ExplainDetail... explainDetails) {
        return planner.explainPlan(this, explainDetails);
    }

    @Override
    public List<String> getSinkIdentifiers() {
        return this.execNodeGraph.getRootNodes().stream()
                .filter(execNode -> execNode instanceof StreamExecSink)
                .map(
                        execNode ->
                                ((StreamExecSink) execNode)
                                        .getTableSinkSpec()
                                        .getContextResolvedTable()
                                        .getIdentifier())
                .map(ObjectIdentifier::asSummaryString)
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return explain();
    }
}

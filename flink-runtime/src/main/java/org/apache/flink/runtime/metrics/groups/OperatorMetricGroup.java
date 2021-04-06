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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Special {@link org.apache.flink.metrics.MetricGroup} representing an Operator. */
@Internal
public class OperatorMetricGroup extends ComponentMetricGroup<TaskMetricGroup> {
    private final String operatorName;
    private final OperatorID operatorID;

    private final OperatorIOMetricGroup ioMetrics;

    public OperatorMetricGroup(
            MetricRegistry registry,
            TaskMetricGroup parent,
            OperatorID operatorID,
            String operatorName) {
        super(
                registry,
                registry.getScopeFormats()
                        .getOperatorFormat()
                        .formatScope(checkNotNull(parent), operatorID, operatorName),
                parent);
        this.operatorID = operatorID;
        this.operatorName = operatorName;

        ioMetrics = new OperatorIOMetricGroup(this);
    }

    // ------------------------------------------------------------------------

    public final TaskMetricGroup parent() {
        return parent;
    }

    @Override
    protected QueryScopeInfo.OperatorQueryScopeInfo createQueryServiceMetricInfo(
            CharacterFilter filter) {
        return new QueryScopeInfo.OperatorQueryScopeInfo(
                this.parent.parent.jobId.toString(),
                this.parent.vertexId.toString(),
                this.parent.subtaskIndex,
                filter.filterCharacters(this.operatorName));
    }

    /**
     * Returns the OperatorIOMetricGroup for this operator.
     *
     * @return OperatorIOMetricGroup for this operator.
     */
    public OperatorIOMetricGroup getIOMetricGroup() {
        return ioMetrics;
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_OPERATOR_ID, String.valueOf(operatorID));
        variables.put(ScopeFormat.SCOPE_OPERATOR_NAME, operatorName);
        // we don't enter the subtask_index as the task group does that already
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return Collections.emptyList();
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "operator";
    }
}

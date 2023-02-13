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

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.util.AbstractID;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing everything belonging to the
 * components running on the JobManager of an Operator.
 */
public class JobManagerOperatorMetricGroup extends ComponentMetricGroup<JobManagerJobMetricGroup> {
    private final AbstractID vertexId;
    private final String taskName;
    private final String operatorName;
    private final OperatorID operatorID;

    public JobManagerOperatorMetricGroup(
            MetricRegistry registry,
            JobManagerJobMetricGroup parent,
            AbstractID vertexId,
            String taskName,
            OperatorID operatorID,
            String operatorName) {
        super(
                registry,
                registry.getScopeFormats()
                        .getJmOperatorFormat()
                        .formatScope(
                                checkNotNull(parent), vertexId, taskName, operatorID, operatorName),
                parent);
        this.vertexId = vertexId;
        this.taskName = taskName;
        this.operatorID = operatorID;
        this.operatorName = operatorName;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "operator";
    }

    @Override
    protected QueryScopeInfo.JobManagerOperatorQueryScopeInfo createQueryServiceMetricInfo(
            CharacterFilter filter) {
        return new QueryScopeInfo.JobManagerOperatorQueryScopeInfo(
                this.parent.jobId.toString(),
                vertexId.toString(),
                filter.filterCharacters(this.operatorName));
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_TASK_VERTEX_ID, vertexId.toString());
        variables.put(ScopeFormat.SCOPE_TASK_NAME, taskName);
        variables.put(ScopeFormat.SCOPE_OPERATOR_ID, String.valueOf(operatorID));
        variables.put(ScopeFormat.SCOPE_OPERATOR_NAME, operatorName);
    }

    @Override
    public void close() {
        super.close();

        parent.removeOperatorMetricGroup(operatorID, operatorName);
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return Collections.emptyList();
    }
}

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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.metrics.groups.TaskMetricGroup.METRICS_OPERATOR_NAME_MAX_LENGTH;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing everything belonging to a
 * specific job, running on the JobManager.
 */
@Internal
public class JobManagerJobMetricGroup extends JobMetricGroup<JobManagerMetricGroup> {
    private final Map<String, JobManagerOperatorMetricGroup> operators = new HashMap<>();

    JobManagerJobMetricGroup(
            MetricRegistry registry,
            JobManagerMetricGroup parent,
            JobID jobId,
            @Nullable String jobName) {
        super(
                registry,
                checkNotNull(parent),
                jobId,
                jobName,
                registry.getScopeFormats()
                        .getJobManagerJobFormat()
                        .formatScope(checkNotNull(parent), jobId, jobName));
    }

    public final JobManagerMetricGroup parent() {
        return parent;
    }

    public JobManagerOperatorMetricGroup getOrAddOperator(
            AbstractID vertexId, String taskName, OperatorID operatorID, String operatorName) {
        final String truncatedOperatorName = getTruncatedOperatorName(operatorName);

        // unique OperatorIDs only exist in streaming, so we have to rely on the name for batch
        // operators
        final String key = operatorID + truncatedOperatorName;

        synchronized (this) {
            return operators.computeIfAbsent(
                    key,
                    operator ->
                            new JobManagerOperatorMetricGroup(
                                    this.registry,
                                    this,
                                    vertexId,
                                    taskName,
                                    operatorID,
                                    truncatedOperatorName));
        }
    }

    private String getTruncatedOperatorName(String operatorName) {
        if (operatorName != null && operatorName.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
            LOG.warn(
                    "The operator name {} exceeded the {} characters length limit and was truncated.",
                    operatorName,
                    METRICS_OPERATOR_NAME_MAX_LENGTH);
            return operatorName.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
        } else {
            return operatorName;
        }
    }

    @VisibleForTesting
    int numRegisteredOperatorMetricGroups() {
        return operators.size();
    }

    void removeOperatorMetricGroup(OperatorID operatorID, String operatorName) {
        final String truncatedOperatorName = getTruncatedOperatorName(operatorName);

        // unique OperatorIDs only exist in streaming, so we have to rely on the name for batch
        // operators
        final String key = operatorID + truncatedOperatorName;

        synchronized (this) {
            if (!isClosed()) {
                operators.remove(key);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return operators.values();
    }
}

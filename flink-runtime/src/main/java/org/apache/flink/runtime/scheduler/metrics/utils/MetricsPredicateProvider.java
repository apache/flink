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

package org.apache.flink.runtime.scheduler.metrics.utils;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobType;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.Predicate;

/** Helper class which provides relevant predicates for state metrics. */
public class MetricsPredicateProvider {

    /** Get the predicate which indicates start of metric collection. */
    public static Predicate<JobExecutionStatsHolder> getStartPredicate(
            JobType semantic,
            ExecutionState targetState,
            EnumSet<ExecutionState> previousStates,
            EnumSet<ExecutionState> nextStates) {
        Predicate<JobExecutionStatsHolder> resultPredicate;
        if (semantic == JobType.BATCH) {
            resultPredicate = getJobExecutionStatsHolderPredicate(targetState, nextStates);
        } else {
            resultPredicate = getJobExecutionStatsHolderPredicate(targetState, previousStates);
        }

        return resultPredicate;
    }

    /** Get the job execution stats holder. */
    private static Predicate<JobExecutionStatsHolder> getJobExecutionStatsHolderPredicate(
            ExecutionState targetState, EnumSet<ExecutionState> previousStates) {
        Predicate<JobExecutionStatsHolder> resultPredicate;
        resultPredicate =
                jobExecutionStatsHolder -> {
                    if (jobExecutionStatsHolder.getStateMetric(targetState) <= 0) {
                        return false;
                    }
                    for (ExecutionState ex : previousStates) {
                        int metric = jobExecutionStatsHolder.getStateMetric(ex);

                        if (metric != 0) {
                            return false;
                        }
                    }

                    return true;
                };
        return resultPredicate;
    }

    /** Get the predicate which marks end of the metrics collection. */
    public static Predicate<JobExecutionStatsHolder> getEndPredicate(
            JobType semantic, EnumSet<ExecutionState> nextStates, Set expectedDeployments) {
        Predicate<JobExecutionStatsHolder> resultPredicate;

        if (semantic == JobType.BATCH) {
            resultPredicate =
                    jobExecutionStatsHolder -> {
                        for (ExecutionState ex : nextStates) {
                            int metric = jobExecutionStatsHolder.getStateMetric(ex);

                            if (metric > 0) {
                                return true;
                            }
                        }

                        return false;
                    };
        } else {
            resultPredicate =
                    jobExecutionStatsHolder -> {
                        int sumOfMetrics = 0;
                        for (ExecutionState ex : nextStates) {
                            sumOfMetrics += jobExecutionStatsHolder.getStateMetric(ex);
                        }

                        if (sumOfMetrics == expectedDeployments.size()) {
                            return true;
                        }

                        return false;
                    };
        }

        return resultPredicate;
    }
}

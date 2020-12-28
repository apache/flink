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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

/**
 * A distribution pattern determines, which sub tasks of a producing task are connected to which
 * consuming sub tasks.
 */
public enum DistributionPattern {

    /**
     * Each producing sub task is connected to each sub task of the consuming task.
     *
     * <p>{@link
     * ExecutionVertex#connectAllToAll(org.apache.flink.runtime.executiongraph.IntermediateResultPartition[],
     * int)}
     */
    ALL_TO_ALL,

    /**
     * Each producing sub task is connected to one or more subtask(s) of the consuming task.
     *
     * <p>{@link
     * ExecutionVertex#connectPointwise(org.apache.flink.runtime.executiongraph.IntermediateResultPartition[],
     * int)}
     */
    POINTWISE
}

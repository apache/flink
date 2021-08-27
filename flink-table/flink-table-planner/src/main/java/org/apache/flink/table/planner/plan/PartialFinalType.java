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

/**
 * Enumerations for partial final aggregate types.
 *
 * @see org.apache.flink.table.planner.plan.rules.logical.SplitAggregateRule
 */
public enum PartialFinalType {
    /**
     * partial aggregate type represents partial-aggregation, which produces a partial distinct
     * aggregated result based on group key and bucket number.
     */
    PARTIAL,
    /**
     * final aggregate type represents final-aggregation, which produces final result based on the
     * partially distinct aggregated result.
     */
    FINAL,
    /** the aggregate which has not been split. */
    NONE
}

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

export interface TopNMetrics {
  topCpuConsumers: CpuConsumerInfo[];
  topBackpressureOperators: BackpressureOperatorInfo[];
  topGcIntensiveTasks: GcTaskInfo[];
  topBusyOperators: BusyOperatorInfo[];
  topLaggingSources: SourceLagInfo[];
}

export interface CpuConsumerInfo {
  /**
   * `null` when the reading is TaskManager-scoped (e.g. JVM process CPU load). Otherwise the
   * originating subtask index.
   */
  subtaskId: number | null;
  taskName: string;
  operatorName: string;
  cpuPercentage: number;
  taskManagerId: string;
}

export interface BackpressureOperatorInfo {
  operatorId: string;
  operatorName: string;
  backpressureRatio: number;
  subtaskId: number;
}

export interface GcTaskInfo {
  taskId: string;
  taskName: string;
  gcTimePercentage: number;
  taskManagerId: string;
}

export interface BusyOperatorInfo {
  operatorId: string;
  operatorName: string;
  busyRatio: number;
  subtaskId: number;
}

/**
 * Lagging source vertex aggregates. Any of the three metric fields may be `null` when the
 * source connector does not expose that metric (e.g. `pendingRecords` or event-time lags).
 * `null` should be rendered as "n/a" in the UI and MUST NOT be conflated with a legitimate 0.
 */
export interface SourceLagInfo {
  vertexId: string;
  vertexName: string;
  pendingRecords: number | null;
  maxFetchEventTimeLagMs: number | null;
  maxEmitEventTimeLagMs: number | null;
}

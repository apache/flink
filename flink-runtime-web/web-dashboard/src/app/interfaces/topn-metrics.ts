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
}

export interface CpuConsumerInfo {
  subtaskId: number;
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

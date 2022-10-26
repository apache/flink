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

interface JobVertexMetricFlags {
  'read-bytes-complete': boolean;
  'read-records-complete': boolean;
  'write-bytes-complete': boolean;
  'write-records-complete': boolean;
}

export interface JobVertexMetricStatistics<T> {
  'read-bytes': T;
  'read-records': T;
  'write-bytes': T;
  'write-records': T;
  'accumulated-backpressured-time': T;
  'accumulated-busy-time': T;
  'accumulated-idle-time': T;
}

export interface AggregatedStatistics {
  min: number;
  max: number;
  avg: number;
  sum: number;
  median: number;
  p25: number;
  p75: number;
  p95: number;
}

export enum JobVertexStatus {
  CREATED = 'CREATED',
  SCHEDULED = 'SCHEDULED',
  DEPLOYING = 'DEPLOYING',
  INITIALIZING = 'INITIALIZING',
  RUNNING = 'RUNNING'
}

export interface JobVertexStatusDuration<T> {
  [JobVertexStatus.CREATED]: T;
  [JobVertexStatus.INITIALIZING]: T;
  [JobVertexStatus.RUNNING]: T;
  [JobVertexStatus.SCHEDULED]: T;
  [JobVertexStatus.DEPLOYING]: T;
}

export interface JobVertexAggregated {
  metrics: JobVertexMetricStatistics<AggregatedStatistics>;
  'status-duration': JobVertexStatusDuration<AggregatedStatistics>;
}

export interface JobVertexSubTaskData {
  attempt: number;
  duration: number;
  'end-time': number;
  host: string;
  start_time: number;
  status: string;
  subtask: number;
  metrics: JobVertexMetricStatistics<number> & JobVertexMetricFlags;
  'taskmanager-id': string;
  'status-duration': JobVertexStatusDuration<number>;
}

export interface JobVertexSubTask extends JobVertexSubTaskData {
  'other-concurrent-attempts'?: JobVertexSubTaskData[];
}

export interface JobVertexSubTaskDetail {
  subtasks: JobVertexSubTask[];
  aggregated: JobVertexAggregated;
}

export interface JobVertexTaskManager {
  id: string;
  name: string;
  now: number;
  taskmanagers: VertexTaskManagerDetail[];
}

export interface VertexTaskManagerDetail {
  duration: number;
  host: string;
  status: string;
  'start-time': number;
  'end-time': number;
  'taskmanager-id': string;
  metrics: JobVertexMetricStatistics<number> & JobVertexMetricFlags;
  'status-counts': {
    CANCELED: number;
    CANCELING: number;
    CREATED: number;
    DEPLOYING: number;
    FAILED: number;
    FINISHED: number;
    RECONCILING: number;
    RUNNING: number;
    SCHEDULED: number;
    INITIALIZING: number;
  };
  aggregated: JobVertexAggregated;
}

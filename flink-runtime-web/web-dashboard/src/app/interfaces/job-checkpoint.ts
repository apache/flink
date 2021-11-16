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

export interface CheckPoint {
  counts: {
    restored: number;
    total: number;
    in_progress: number;
    completed: number;
    failed: number;
  };
  summary: {
    state_size: CheckPointMinMaxAvgStatistics;
    end_to_end_duration: CheckPointMinMaxAvgStatistics;
    alignment_buffered: CheckPointMinMaxAvgStatistics;
  };
  latest: {
    completed: CheckPointCompletedStatistics;
    savepoint: CheckPointCompletedStatistics;
    failed: {
      id: number;
      status: string;
      is_savepoint: boolean;
      trigger_timestamp: number;
      latest_ack_timestamp: number;
      state_size: number;
      end_to_end_duration: number;
      alignment_buffered: number;
      num_subtasks: number;
      num_acknowledged_subtasks: number;
      failure_timestamp: number;
      failure_message: string;
      task: CheckPointTaskStatistics;
    };
    restored: {
      id: number;
      restore_timestamp: number;
      is_savepoint: boolean;
      external_path: string;
    };
    history: CheckPointHistory;
  };
}

export interface CheckPointHistory {
  id: number;
  status: string;
  is_savepoint: boolean;
  trigger_timestamp: number;
  latest_ack_timestamp: number;
  state_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
  task: CheckPointTaskStatistics;
}

export interface CheckPointMinMaxAvgStatistics {
  min: number;
  max: number;
  avg: number;
  p50: number;
  p90: number;
  p95: number;
  p99: number;
  p999: number;
}

export interface CheckPointCompletedStatistics {
  id: number;
  status: string;
  is_savepoint: boolean;
  trigger_timestamp: number;
  latest_ack_timestamp: number;
  state_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
  tasks: CheckPointTaskStatistics;
  external_path: string;
  discarded: boolean;
  checkpoint_type: string;
}

export interface CheckPointTaskStatistics {
  id: number;
  status: string;
  latest_ack_timestamp: number;
  state_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
}

export interface CheckPointConfig {
  mode: 'exactly_once' | string;
  interval: number;
  timeout: number;
  min_pause: number;
  max_concurrent: number;
  externalization: {
    enabled: boolean;
    delete_on_cancellation: boolean;
  };
  state_backend: string;
  checkpoint_storage: string;
  unaligned_checkpoints: boolean;
  tolerable_failed_checkpoints: number;
  aligned_checkpoint_timeout: number;
  checkpoints_after_tasks_finish: boolean;
}

export interface CheckPointDetail {
  id: number;
  status: string;
  is_savepoint: boolean;
  trigger_timestamp: number;
  latest_ack_timestamp: number;
  state_size: number;
  end_to_end_duration: number;
  external_path: string;
  discarded: boolean;
  alignment_buffered: number;
  failure_message?: string;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
  checkpoint_type: string;
  tasks: Array<{
    [taskId: string]: {
      id: number;
      status: string;
      latest_ack_timestamp: number;
      state_size: number;
      end_to_end_duration: number;
      alignment_buffered: number;
      num_subtasks: number;
      num_acknowledged_subtasks: number;
    };
  }>;
}

export interface CompletedSubTaskCheckPointStatistics {
  ack_timestamp: number;
  end_to_end_duration: number;
  state_size: number;
  checkpoint: {
    sync: number;
    async: number;
  };
  alignment: {
    buffer: number;
    processed: number;
    persisted: number;
    duration: number;
  };
  start_delay: number;
  unaligned_checkpoint: boolean;
  aborted: boolean;
}

export interface PendingSubTaskCheckPointStatistics {}

export type SubTaskCheckPointStatisticsItem = {
  index: number;
  status: string;
} & (CompletedSubTaskCheckPointStatistics | PendingSubTaskCheckPointStatistics);

export interface CheckPointSubTask {
  id: number;
  status: string;
  latest_ack_timestamp: number;
  state_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
  summary: {
    state_size: CheckPointMinMaxAvgStatistics;
    end_to_end_duration: CheckPointMinMaxAvgStatistics;
    checkpoint_duration: {
      sync: CheckPointMinMaxAvgStatistics;
      async: CheckPointMinMaxAvgStatistics;
    };
    alignment: {
      buffered: CheckPointMinMaxAvgStatistics;
      duration: CheckPointMinMaxAvgStatistics;
    };
    start_delay: CheckPointMinMaxAvgStatistics;
  };
  subtasks: SubTaskCheckPointStatisticsItem[];
}

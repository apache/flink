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

export interface Checkpoint {
  counts: {
    restored: number;
    total: number;
    in_progress: number;
    completed: number;
    failed: number;
  };
  summary: {
    checkpointed_size: CheckpointMinMaxAvgStatistics;
    state_size: CheckpointMinMaxAvgStatistics;
    end_to_end_duration: CheckpointMinMaxAvgStatistics;
    processed_data: CheckpointMinMaxAvgStatistics;
    persisted_data: CheckpointMinMaxAvgStatistics;
    alignment_buffered: CheckpointMinMaxAvgStatistics;
  };
  latest: {
    completed: CheckpointCompletedStatistics;
    savepoint: CheckpointCompletedStatistics;
    failed: {
      id: number;
      status: string;
      is_savepoint: boolean;
      trigger_timestamp: number;
      latest_ack_timestamp: number;
      state_size: number;
      checkpointed_size: number;
      end_to_end_duration: number;
      alignment_buffered: number;
      num_subtasks: number;
      num_acknowledged_subtasks: number;
      failure_timestamp: number;
      failure_message: string;
      task: CheckpointTaskStatistics;
    };
    restored: {
      id: number;
      restore_timestamp: number;
      is_savepoint: boolean;
      external_path: string;
    };
    history: CheckpointHistory;
  };
}

export interface CheckpointHistory {
  id: number;
  status: string;
  is_savepoint: boolean;
  trigger_timestamp: number;
  latest_ack_timestamp: number;
  state_size: number;
  checkpointed_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
  task: CheckpointTaskStatistics;
}

export interface CheckpointMinMaxAvgStatistics {
  min: number;
  max: number;
  avg: number;
  p50: number;
  p90: number;
  p95: number;
  p99: number;
  p999: number;
}

export interface CheckpointCompletedStatistics {
  id: number;
  status: string;
  is_savepoint: boolean;
  trigger_timestamp: number;
  latest_ack_timestamp: number;
  state_size: number;
  checkpointed_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
  tasks: CheckpointTaskStatistics;
  external_path: string;
  discarded: boolean;
  checkpoint_type: string;
}

export interface CheckpointTaskStatistics {
  id: number;
  status: string;
  latest_ack_timestamp: number;
  state_size: number;
  checkpointed_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
}

export interface CheckpointConfig {
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
  state_changelog_enabled: boolean;
  checkpoint_storage: string;
  unaligned_checkpoints: boolean;
  tolerable_failed_checkpoints: number;
  aligned_checkpoint_timeout: number;
  checkpoints_after_tasks_finish: boolean;
  changelog_storage: string;
  changelog_periodic_materialization_interval: number;
}

export interface CheckpointDetail {
  id: number;
  status: string;
  is_savepoint: boolean;
  savepointFormat: string;
  trigger_timestamp: number;
  latest_ack_timestamp: number;
  state_size: number;
  checkpointed_size: number;
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
      checkpointed_size: number;
      end_to_end_duration: number;
      alignment_buffered: number;
      num_subtasks: number;
      num_acknowledged_subtasks: number;
    };
  }>;
}

export interface CompletedSubTaskCheckpointStatistics {
  ack_timestamp: number;
  end_to_end_duration: number;
  checkpointed_size: number;
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

export interface PendingSubTaskCheckpointStatistics {}

export type SubTaskCheckpointStatisticsItem = {
  index: number;
  status: string;
} & (CompletedSubTaskCheckpointStatistics | PendingSubTaskCheckpointStatistics);

export interface CheckpointSubTask {
  id: number;
  status: string;
  latest_ack_timestamp: number;
  state_size: number;
  checkpointed_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
  summary: {
    checkpointed_size: CheckpointMinMaxAvgStatistics;
    state_size: CheckpointMinMaxAvgStatistics;
    end_to_end_duration: CheckpointMinMaxAvgStatistics;
    checkpoint_duration: {
      sync: CheckpointMinMaxAvgStatistics;
      async: CheckpointMinMaxAvgStatistics;
    };
    alignment: {
      buffered: CheckpointMinMaxAvgStatistics;
      duration: CheckpointMinMaxAvgStatistics;
    };
    start_delay: CheckpointMinMaxAvgStatistics;
  };
  subtasks: SubTaskCheckpointStatisticsItem[];
}

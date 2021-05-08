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

export interface CheckPointInterface {
  counts: {
    restored: number;
    total: number;
    in_progress: number;
    completed: number;
    failed: number;
  };
  summary: {
    state_size: CheckPointMinMaxAvgStatisticsInterface;
    end_to_end_duration: CheckPointMinMaxAvgStatisticsInterface;
    alignment_buffered: CheckPointMinMaxAvgStatisticsInterface;
  };
  latest: {
    completed: CheckPointCompletedStatisticsInterface;
    savepoint: CheckPointCompletedStatisticsInterface;
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
      task: CheckPointTaskStatisticsInterface;
    };
    restored: {
      id: number;
      restore_timestamp: number;
      is_savepoint: boolean;
      external_path: string;
    };
    history: CheckPointHistoryInterface;
  };
}

export interface CheckPointHistoryInterface {
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
  task: CheckPointTaskStatisticsInterface;
}

export interface CheckPointMinMaxAvgStatisticsInterface {
  min: number;
  max: number;
  avg: number;
}

export interface CheckPointCompletedStatisticsInterface {
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
  tasks: CheckPointTaskStatisticsInterface;
  external_path: string;
  discarded: boolean;
  checkpoint_type: string;
}

export interface CheckPointTaskStatisticsInterface {
  id: number;
  status: string;
  latest_ack_timestamp: number;
  state_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
}

export interface CheckPointConfigInterface {
  mode: any;
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
  alignment_timeout: number;
}

export interface CheckPointDetailInterface {
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
    [ taskId: string ]: {
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

export interface CheckPointSubTaskInterface {
  id: number;
  status: string;
  latest_ack_timestamp: number;
  state_size: number;
  end_to_end_duration: number;
  alignment_buffered: number;
  num_subtasks: number;
  num_acknowledged_subtasks: number;
  summary: {
    state_size: CheckPointMinMaxAvgStatisticsInterface;
    end_to_end_duration: CheckPointMinMaxAvgStatisticsInterface;
    checkpoint_duration: {
      sync: CheckPointMinMaxAvgStatisticsInterface;
      async: CheckPointMinMaxAvgStatisticsInterface;
    };
    alignment: {
      buffered: CheckPointMinMaxAvgStatisticsInterface;
      duration: CheckPointMinMaxAvgStatisticsInterface;
    };
    start_delay: CheckPointMinMaxAvgStatisticsInterface;
  };
  subtasks: Array<{
    index: number;
    status: string;
  }>;
}

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

// Test data shared by the timeline specs.

import {
  CheckpointHistory,
  CheckpointSubTask,
  SubTaskCheckpointStatisticsItem,
  VerticesItem
} from '@flink-runtime-web/interfaces';

export const TRIGGERED = 1_000;

export interface Phases {
  start_delay?: number;
  alignment?: number;
  sync?: number;
  async?: number;
}

export const completed = (index: number, phases: Phases, aborted = false): SubTaskCheckpointStatisticsItem =>
  ({
    index,
    status: 'completed',
    ack_timestamp: 0,
    end_to_end_duration: 0,
    checkpointed_size: 0,
    state_size: 0,
    checkpoint: { sync: phases.sync ?? 0, async: phases.async ?? 0 },
    alignment: { buffer: 0, processed: 0, persisted: 0, duration: phases.alignment ?? 0 },
    start_delay: phases.start_delay ?? 0,
    unaligned_checkpoint: false,
    aborted
  }) as SubTaskCheckpointStatisticsItem;

export const noStats = (index: number): SubTaskCheckpointStatisticsItem =>
  ({ index, status: 'pending_or_failed' }) as SubTaskCheckpointStatisticsItem;

export const details = (...subtasks: SubTaskCheckpointStatisticsItem[]): CheckpointSubTask =>
  ({ subtasks }) as unknown as CheckpointSubTask;

export const vertex = (id: string, name: string): VerticesItem => ({ id, name, status: 'RUNNING' }) as VerticesItem;

export const historyEntry = (id: number, status: string, acked: number, isSavepoint = false): CheckpointHistory =>
  ({
    id,
    status,
    is_savepoint: isSavepoint,
    trigger_timestamp: TRIGGERED,
    end_to_end_duration: 5,
    num_acknowledged_subtasks: acked,
    num_subtasks: 4,
    state_size: 0
  }) as CheckpointHistory;

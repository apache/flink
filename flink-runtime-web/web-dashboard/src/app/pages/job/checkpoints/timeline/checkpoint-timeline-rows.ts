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

import {
  CheckpointConfig,
  CheckpointHistory,
  CheckpointSubTask,
  CompletedSubTaskCheckpointStatistics,
  SubTaskCheckpointStatisticsItem,
  VerticesItem
} from '@flink-runtime-web/interfaces';

export type PhaseKey = 'start_delay' | 'alignment' | 'sync' | 'async';

// One bar of the recent-checkpoints strip, and the checkpoint the timeline below it shows.
export interface StripDatum {
  id: number;
  idLabel: string;
  duration: number;
  status: 'COMPLETED' | 'SAVEPOINT' | 'IN_PROGRESS' | 'FAILED';
  rawStatus: string;
  isSavepoint: boolean;
  triggered: number;
  acked: string;
  size: number;
}

export interface TimelineDatum {
  row: string;
  operatorFull: string;
  subtask: number;
  phase: PhaseKey;
  phaseLabel: string;
  range: [number, number];
  durationMs: number;
  aborted: boolean;
  stateSize: number;
  checkpointedSize: number;
  totalDuration: number;
}

// A subtask that reported no stats for the checkpoint, drawn as one hatched bar spanning it.
export interface UnackedDatum {
  row: string;
  operatorFull: string;
  subtask: number;
  label: 'no ack' | 'pending';
  range: [number, number];
}

// A subtask whose phases all rounded to 0ms, drawn as a hairline at trigger time so its row isn't empty.
export interface InstantDatum {
  row: string;
  operatorFull: string;
  subtask: number;
  aborted: boolean;
  stateSize: number;
  checkpointedSize: number;
  range: [number, number];
}

export interface TimelineRows {
  rows: TimelineDatum[];
  unacked: UnackedDatum[];
  instants: InstantDatum[];
  rowOrder: string[];
}

// What building the rows reads from a strip bar.
export type TimelineCheckpoint = Pick<StripDatum, 'duration' | 'rawStatus' | 'isSavepoint' | 'triggered'>;

const OPERATOR_NAME_MAX = 60;

export function toStripDatum(h: CheckpointHistory): StripDatum {
  // FAILED first, so a failed savepoint shows as failed rather than as a savepoint.
  const status: StripDatum['status'] =
    h.status === 'FAILED'
      ? 'FAILED'
      : h.is_savepoint
        ? 'SAVEPOINT'
        : h.status === 'IN_PROGRESS'
          ? 'IN_PROGRESS'
          : 'COMPLETED';
  return {
    id: h.id,
    idLabel: String(h.id),
    duration: Math.max(h.end_to_end_duration || 0, 0),
    status,
    rawStatus: h.status,
    isSavepoint: h.is_savepoint,
    triggered: h.trigger_timestamp,
    acked: `${h.num_acknowledged_subtasks}/${h.num_subtasks}`,
    size: h.state_size
  };
}

// One row per subtask, in operator then subtask order, with a segment per checkpoint phase.
export function buildTimelineRows(
  datum: TimelineCheckpoint,
  vertices: VerticesItem[],
  perVertex: Record<string, CheckpointSubTask | null>,
  config?: CheckpointConfig
): TimelineRows {
  const base = datum.triggered;
  // A completed checkpoint was acked by every running subtask, so missing stats there are not a failure.
  const unackedLabel =
    datum.rawStatus === 'FAILED' ? 'no ack' : datum.rawStatus === 'IN_PROGRESS' ? 'pending' : undefined;
  const rows: TimelineDatum[] = [];
  const unacked: UnackedDatum[] = [];
  const instants: InstantDatum[] = [];
  const rowOrder: string[] = [];
  let maxTotal = 0;
  const rowPrefixOwner = new Map<string, string>();

  for (const vertex of vertices) {
    const cp = perVertex[vertex.id];
    if (!cp || !cp.subtasks) {
      continue;
    }
    const opNameFull = (vertex.name || '').replace(/\s+/g, ' ').trim();
    let opName = shortenOperatorName(vertex.name);
    // shortenOperatorName truncates, so distinct vertices can collide; the row key must stay unique.
    const owner = rowPrefixOwner.get(opName);
    if (owner !== undefined && owner !== vertex.id) {
      opName = `${opName} [${vertex.id.slice(0, 4)}]`;
    }
    rowPrefixOwner.set(opName, vertex.id);

    // By index, rather than in the order the response lists them.
    for (const s of [...cp.subtasks].sort((x, y) => x.index - y.index)) {
      const row = `${opName} #${s.index}`;
      if (!isCompletedSubtask(s)) {
        // Finished tasks never land here: the JobManager records them as acknowledged when it triggers.
        if (unackedLabel) {
          unacked.push({ row, operatorFull: opNameFull, subtask: s.index, label: unackedLabel, range: [base, base] });
          rowOrder.push(row);
        }
        continue;
      }
      const startDelay = Math.max(s.start_delay || 0, 0);
      const reportedAlignment = Math.max(s.alignment.duration || 0, 0);
      const alignment = alignmentBeforeSync(datum, config, startDelay, reportedAlignment);
      const alignmentInAsync = reportedAlignment - alignment;
      const sync = Math.max(s.checkpoint.sync || 0, 0);
      const asyncMs = Math.max(s.checkpoint.async || 0, 0);
      const total = startDelay + alignment + sync + asyncMs;
      rowOrder.push(row);
      maxTotal = Math.max(maxTotal, total);
      if (total === 0) {
        instants.push({
          row,
          operatorFull: opNameFull,
          subtask: s.index,
          aborted: !!s.aborted,
          stateSize: s.state_size,
          checkpointedSize: s.checkpointed_size,
          range: [base, base]
        });
        continue;
      }

      const segs: Array<[PhaseKey, number]> = [
        ['start_delay', startDelay],
        ['alignment', alignment],
        ['sync', sync],
        ['async', asyncMs]
      ];
      let cursor = base;
      for (const [phase, ms] of segs) {
        if (ms > 0) {
          rows.push({
            row,
            operatorFull: opNameFull,
            subtask: s.index,
            phase,
            phaseLabel:
              phase === 'async' && alignmentInAsync > 0 ? `${phase} (incl. ${alignmentInAsync}ms alignment)` : phase,
            range: [cursor, cursor + ms],
            durationMs: ms,
            aborted: !!s.aborted,
            stateSize: s.state_size,
            checkpointedSize: s.checkpointed_size,
            totalDuration: total
          });
        }
        cursor += ms;
      }
    }
  }

  // Unacked subtasks span the whole checkpoint: until it failed, or the latest ack so far while in progress.
  const unackedEnd = base + Math.max(datum.duration, maxTotal, 1);
  for (const u of unacked) {
    u.range = [base, unackedEnd];
  }

  return { rows, unacked, instants, rowOrder };
}

// How much of a subtask's alignment came before its sync phase. With unaligned checkpoints, a checkpoint starts
// aligned and switches to unaligned once aligned_checkpoint_timeout has passed since the trigger; after that the
// rest of the alignment runs during the async phase, whose duration includes it (checkpoint_monitoring.md).
// Savepoints and at-least-once checkpoints never switch. The per-subtask unaligned_checkpoint flag can't tell: it
// only says whether in-flight data was persisted.
function alignmentBeforeSync(
  datum: TimelineCheckpoint,
  config: CheckpointConfig | undefined,
  startDelay: number,
  alignment: number
): number {
  if (!config?.unaligned_checkpoints || config.mode !== 'exactly_once' || datum.isSavepoint) {
    return alignment;
  }
  const timeout = config.aligned_checkpoint_timeout;
  // 0 and Long.MAX_VALUE both mean unaligned from the first barrier (CheckpointOptions.forConfig).
  if (!(timeout > 0) || timeout >= Number.MAX_SAFE_INTEGER) {
    return 0;
  }
  return Math.max(0, Math.min(alignment, timeout - startDelay));
}

// Rows by when they finished, latest first; ties keep operator then subtask order (the sort is stable). Subtasks that
// never finished come first: those without an ack, and aborted ones, whose reported phases stop short of where they
// were when the checkpoint failed (they are why it failed).
export function slowestFirstOrder({ rows, unacked, instants, rowOrder }: TimelineRows): string[] {
  const end = new Map<string, number>();
  for (const d of [...rows, ...instants]) {
    end.set(d.row, d.aborted ? Infinity : Math.max(end.get(d.row) ?? -Infinity, d.range[1]));
  }
  for (const d of unacked) {
    end.set(d.row, Infinity);
  }
  return [...rowOrder].sort((a, b) => {
    const [ea, eb] = [end.get(a) ?? 0, end.get(b) ?? 0];
    return ea === eb ? 0 : eb > ea ? 1 : -1;
  });
}

// Backend tags completed subtasks "completed" (incl. aborted) and others "pending_or_failed".
function isCompletedSubtask(
  s: SubTaskCheckpointStatisticsItem
): s is SubTaskCheckpointStatisticsItem & CompletedSubTaskCheckpointStatistics {
  return s.status === 'completed';
}

function shortenOperatorName(name: string): string {
  if (!name) {
    return '';
  }
  const compact = name.replace(/\s+/g, ' ').trim();
  return compact.length <= OPERATOR_NAME_MAX ? compact : `${compact.slice(0, OPERATOR_NAME_MAX - 3)}...`;
}

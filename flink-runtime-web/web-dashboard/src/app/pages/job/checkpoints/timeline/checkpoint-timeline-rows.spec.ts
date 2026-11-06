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

import { CheckpointConfig, CheckpointHistory, CheckpointSubTask, VerticesItem } from '@flink-runtime-web/interfaces';
import { describe, expect, it } from 'vitest';

import { buildTimelineRows, TimelineCheckpoint, TimelineRows, toStripDatum } from './checkpoint-timeline-rows';
import { completed, details, noStats, TRIGGERED, vertex } from './checkpoint-timeline.testing';

const checkpoint = (rawStatus: string, duration: number, isSavepoint = false): TimelineCheckpoint => ({
  duration,
  rawStatus,
  isSavepoint,
  triggered: TRIGGERED
});

const build = (
  datum: TimelineCheckpoint,
  vertices: VerticesItem[],
  perVertex: Record<string, CheckpointSubTask | null>,
  config?: CheckpointConfig
): TimelineRows => buildTimelineRows(datum, vertices, perVertex, config);

const unalignedConfig = (alignedCheckpointTimeout: number, mode = 'exactly_once'): CheckpointConfig =>
  ({ mode, unaligned_checkpoints: true, aligned_checkpoint_timeout: alignedCheckpointTimeout }) as CheckpointConfig;

const SUBTASK_PHASES = { start_delay: 2, alignment: 5, sync: 1, async: 8 };

const segments = (rows: TimelineRows['rows'], row: string): Array<[string, string, [number, number], number]> =>
  rows.filter(r => r.row === row).map(r => [r.phase, r.phaseLabel, r.range, r.totalDuration]);

describe('buildTimelineRows', () => {
  const agg = [vertex('a', 'Agg')];

  it('orders rows by operator, in job graph order, then by subtask index', () => {
    // Chosen so that every other order differs: the response order, by operator name, by subtask index alone,
    // and by duration either way.
    const { rowOrder } = build(checkpoint('COMPLETED', 60), [vertex('s', 'Source'), vertex('a', 'Agg')], {
      s: details(completed(1, { async: 1 }), completed(0, { async: 30 })),
      a: details(completed(2, { async: 5 }), completed(0, { async: 50 }), completed(1, { async: 20 }))
    });

    expect(rowOrder).toEqual(['Source #0', 'Source #1', 'Agg #0', 'Agg #1', 'Agg #2']);
  });

  it('keeps a row with an instant marker for subtasks whose phases all rounded to 0ms', () => {
    const { instants, rowOrder } = build(checkpoint('COMPLETED', 15), [vertex('s', 'Source')], {
      s: details(completed(0, {}), completed(1, { start_delay: 1 }))
    });

    expect(rowOrder).toEqual(['Source #0', 'Source #1']);
    expect(instants).toEqual([
      expect.objectContaining({ row: 'Source #0', aborted: false, range: [TRIGGERED, TRIGGERED] })
    ]);
  });

  it('draws a no-ack bar spanning a failed checkpoint for subtasks without stats', () => {
    const { unacked, rowOrder } = build(checkpoint('FAILED', 10), agg, {
      a: details(completed(0, { start_delay: 1, async: 3 }, true), noStats(1), completed(2, { async: 2 }))
    });

    expect(unacked).toEqual([
      expect.objectContaining({ row: 'Agg #1', label: 'no ack', range: [TRIGGERED, TRIGGERED + 10] })
    ]);
    expect(rowOrder).toEqual(['Agg #0', 'Agg #1', 'Agg #2']);
  });

  it('marks subtasks without stats as pending up to the latest known progress while in progress', () => {
    const { unacked } = build(checkpoint('IN_PROGRESS', 0), agg, {
      a: details(completed(0, { start_delay: 1, async: 4 }), noStats(1))
    });

    expect(unacked).toEqual([expect.objectContaining({ label: 'pending', range: [TRIGGERED, TRIGGERED + 5] })]);
  });

  it('uses the raw status for savepoints', () => {
    const { unacked } = build(checkpoint('FAILED', 10, true), agg, { a: details(noStats(0)) });

    expect(unacked.map(u => u.label)).toEqual(['no ack']);
  });

  it('ignores missing stats on completed checkpoints', () => {
    const { unacked, rowOrder } = build(checkpoint('COMPLETED', 10), agg, {
      a: details(completed(0, { async: 2 }), noStats(1))
    });

    expect(unacked).toEqual([]);
    expect(rowOrder).toEqual(['Agg #0']);
  });

  it('keeps row keys unique when two operator names shorten to the same label', () => {
    const name = `${'x'.repeat(70)}`;
    const { rowOrder } = build(
      checkpoint('COMPLETED', 10),
      [vertex('aaaa1', `${name}1`), vertex('bbbb2', `${name}2`)],
      {
        aaaa1: details(completed(0, { async: 1 })),
        bbbb2: details(completed(0, { async: 1 }))
      }
    );

    expect(new Set(rowOrder).size).toBe(2);
    expect(rowOrder[1]).toContain('[bbbb]');
  });

  // Long.MAX_VALUE (2 ** 63 once parsed) means no timeout, i.e. unaligned from the first barrier, like 0.
  it.each([0, 2 ** 63])('draws no alignment before sync for checkpoints unaligned from the start (timeout %s)', t => {
    const { rows } = build(
      checkpoint('COMPLETED', 20),
      agg,
      { a: details(completed(0, SUBTASK_PHASES)) },
      unalignedConfig(t)
    );

    expect(segments(rows, 'Agg #0')).toEqual([
      ['start_delay', 'start_delay', [TRIGGERED, TRIGGERED + 2], 11],
      ['sync', 'sync', [TRIGGERED + 2, TRIGGERED + 3], 11],
      ['async', 'async (incl. 5ms alignment)', [TRIGGERED + 3, TRIGGERED + 11], 11]
    ]);
  });

  it('draws the alignment up to the aligned checkpoint timeout before sync', () => {
    const { rows } = build(
      checkpoint('COMPLETED', 40_000),
      agg,
      {
        a: details(
          completed(0, { start_delay: 5_000, alignment: 31_000, sync: 10, async: 8_000 }),
          completed(1, SUBTASK_PHASES)
        )
      },
      unalignedConfig(30_000)
    );

    expect(segments(rows, 'Agg #0')).toEqual([
      ['start_delay', 'start_delay', [TRIGGERED, TRIGGERED + 5_000], 38_010],
      ['alignment', 'alignment', [TRIGGERED + 5_000, TRIGGERED + 30_000], 38_010],
      ['sync', 'sync', [TRIGGERED + 30_000, TRIGGERED + 30_010], 38_010],
      ['async', 'async (incl. 6000ms alignment)', [TRIGGERED + 30_010, TRIGGERED + 38_010], 38_010]
    ]);
    // Aligned within the timeout, so it never switched.
    expect(segments(rows, 'Agg #1').map(([, label]) => label)).toEqual(['start_delay', 'alignment', 'sync', 'async']);
  });

  it.each([
    ['savepoints', unalignedConfig(0), true],
    ['at-least-once checkpoints', unalignedConfig(0, 'at_least_once'), false],
    ['jobs without unaligned checkpoints', { ...unalignedConfig(0), unaligned_checkpoints: false }, false]
  ])('draws the whole alignment before sync for %s', (_, config, isSavepoint) => {
    const { rows } = build(
      checkpoint('COMPLETED', 20, isSavepoint),
      agg,
      { a: details(completed(0, SUBTASK_PHASES)) },
      config
    );

    expect(segments(rows, 'Agg #0')).toEqual([
      ['start_delay', 'start_delay', [TRIGGERED, TRIGGERED + 2], 16],
      ['alignment', 'alignment', [TRIGGERED + 2, TRIGGERED + 7], 16],
      ['sync', 'sync', [TRIGGERED + 7, TRIGGERED + 8], 16],
      ['async', 'async', [TRIGGERED + 8, TRIGGERED + 16], 16]
    ]);
  });
});

describe('toStripDatum', () => {
  const savepoint = (status: string): CheckpointHistory =>
    ({ id: 7, status, is_savepoint: true, num_acknowledged_subtasks: 1, num_subtasks: 4 }) as CheckpointHistory;

  it('draws a failed savepoint as failed, and an in-progress one as a savepoint', () => {
    expect(toStripDatum(savepoint('FAILED')).status).toBe('FAILED');
    expect(toStripDatum(savepoint('IN_PROGRESS')).status).toBe('SAVEPOINT');
    expect(toStripDatum(savepoint('IN_PROGRESS')).acked).toBe('1/4');
  });
});

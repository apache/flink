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

import { HttpErrorResponse } from '@angular/common/http';
import { ChangeDetectorRef, ElementRef, SimpleChange } from '@angular/core';
import { BehaviorSubject, of, Subject, throwError } from 'rxjs';

import {
  CheckpointConfig,
  CheckpointHistory,
  CheckpointSubTask,
  SubTaskCheckpointStatisticsItem,
  VerticesItem
} from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { buildTimelineTicks, JobCheckpointsTimelineComponent } from './job-checkpoints-timeline.component';

const TRIGGERED = 1_000;

interface Phases {
  start_delay?: number;
  alignment?: number;
  sync?: number;
  async?: number;
}

const completed = (
  index: number,
  phases: Phases,
  aborted = false,
  unaligned = false
): SubTaskCheckpointStatisticsItem =>
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
    unaligned_checkpoint: unaligned,
    aborted
  }) as SubTaskCheckpointStatisticsItem;

const noStats = (index: number): SubTaskCheckpointStatisticsItem =>
  ({ index, status: 'pending_or_failed' }) as SubTaskCheckpointStatisticsItem;

const details = (...subtasks: SubTaskCheckpointStatisticsItem[]): CheckpointSubTask =>
  ({ subtasks }) as unknown as CheckpointSubTask;

const vertex = (id: string, name: string): VerticesItem => ({ id, name, status: 'RUNNING' }) as VerticesItem;

const checkpoint = (
  rawStatus: string,
  duration: number,
  isSavepoint = false
): { id: number; duration: number; rawStatus: string; isSavepoint: boolean; triggered: number } => ({
  id: 7,
  duration,
  rawStatus,
  isSavepoint,
  triggered: TRIGGERED
});

type BuildRows = (
  datum: ReturnType<typeof checkpoint>,
  perVertex: Record<string, CheckpointSubTask | null>
) => {
  rows: Array<{ row: string; phase: string; phaseLabel: string; range: [number, number]; totalDuration: number }>;
  unacked: Array<{ row: string; label: string; range: [number, number] }>;
  instants: Array<{ row: string; aborted: boolean; range: [number, number] }>;
  rowOrder: string[];
};

describe('JobCheckpointsTimelineComponent rows', () => {
  let component: JobCheckpointsTimelineComponent;
  let buildRows: BuildRows;

  beforeEach(() => {
    component = new JobCheckpointsTimelineComponent(
      {} as JobService,
      { markForCheck: () => undefined } as unknown as ChangeDetectorRef
    );
    buildRows = (component as unknown as { buildTimelineRows: BuildRows }).buildTimelineRows.bind(component);
  });

  it('keeps rows in operator then subtask order instead of sorting by duration', () => {
    component.vertices = [vertex('a', 'Agg'), vertex('s', 'Sink')];

    const { rowOrder } = buildRows(checkpoint('COMPLETED', 60), {
      a: details(completed(0, { async: 5 }), completed(1, { async: 50 })),
      s: details(completed(0, { start_delay: 1 }))
    });

    expect(rowOrder).toEqual(['Agg #0', 'Agg #1', 'Sink #0']);
  });

  it('keeps a row with an instant marker for subtasks whose phases all rounded to 0ms', () => {
    component.vertices = [vertex('s', 'Source')];

    const { instants, rowOrder } = buildRows(checkpoint('COMPLETED', 15), {
      s: details(completed(0, {}), completed(1, { start_delay: 1 }))
    });

    expect(rowOrder).toEqual(['Source #0', 'Source #1']);
    expect(instants).toEqual([
      expect.objectContaining({ row: 'Source #0', aborted: false, range: [TRIGGERED, TRIGGERED] })
    ]);
  });

  it('draws a no-ack bar spanning a failed checkpoint for subtasks without stats', () => {
    component.vertices = [vertex('a', 'Agg')];

    const { unacked, rowOrder } = buildRows(checkpoint('FAILED', 10), {
      a: details(completed(0, { start_delay: 1, async: 3 }, true), noStats(1), completed(2, { async: 2 }))
    });

    expect(unacked).toEqual([
      expect.objectContaining({ row: 'Agg #1', label: 'no ack', range: [TRIGGERED, TRIGGERED + 10] })
    ]);
    expect(rowOrder).toEqual(['Agg #0', 'Agg #1', 'Agg #2']);
  });

  it('marks subtasks without stats as pending up to the latest known progress while in progress', () => {
    component.vertices = [vertex('a', 'Agg')];

    const { unacked } = buildRows(checkpoint('IN_PROGRESS', 0), {
      a: details(completed(0, { start_delay: 1, async: 4 }), noStats(1))
    });

    expect(unacked).toEqual([expect.objectContaining({ label: 'pending', range: [TRIGGERED, TRIGGERED + 5] })]);
  });

  it('uses the raw status for savepoints', () => {
    component.vertices = [vertex('a', 'Agg')];

    const { unacked } = buildRows(checkpoint('FAILED', 10, true), { a: details(noStats(0)) });

    expect(unacked.map(u => u.label)).toEqual(['no ack']);
  });

  it('ignores missing stats on completed checkpoints', () => {
    component.vertices = [vertex('a', 'Agg')];

    const { unacked, rowOrder } = buildRows(checkpoint('COMPLETED', 10), {
      a: details(completed(0, { async: 2 }), noStats(1))
    });

    expect(unacked).toEqual([]);
    expect(rowOrder).toEqual(['Agg #0']);
  });

  it('does not stack alignment for unaligned subtasks, since their async phase already includes it', () => {
    component.vertices = [vertex('a', 'Agg')];
    const phases = { start_delay: 2, alignment: 5, sync: 1, async: 8 };

    const { rows } = buildRows(checkpoint('COMPLETED', 20), {
      a: details(completed(0, phases, false, true), completed(1, phases))
    });

    const segments = (row: string): Array<[string, string, [number, number], number]> =>
      rows.filter(r => r.row === row).map(r => [r.phase, r.phaseLabel, r.range, r.totalDuration]);
    expect(segments('Agg #0')).toEqual([
      ['start_delay', 'start_delay', [TRIGGERED, TRIGGERED + 2], 11],
      ['sync', 'sync', [TRIGGERED + 2, TRIGGERED + 3], 11],
      ['async', 'async (incl. 5ms alignment)', [TRIGGERED + 3, TRIGGERED + 11], 11]
    ]);
    expect(segments('Agg #1').map(([phase]) => phase)).toEqual(['start_delay', 'alignment', 'sync', 'async']);
  });

  it('does not cache the details of an in-progress savepoint', () => {
    const internals = component as unknown as {
      detailsCache: Map<number, unknown>;
      cacheDetails: (datum: ReturnType<typeof checkpoint> & { status: string }, perVertex: object) => void;
    };

    internals.cacheDetails({ ...checkpoint('IN_PROGRESS', 0, true), status: 'SAVEPOINT' }, {});
    internals.cacheDetails({ ...checkpoint('COMPLETED', 5, true), id: 8, status: 'SAVEPOINT' }, {});

    expect([...internals.detailsCache.keys()]).toEqual([8]);
  });

  it('pluralizes the partial-failure message by the number of requested operators', () => {
    component.failedVertexCount = 1;
    component.requestedVertexCount = 2;

    expect(component.partialFailureMessage).toBe('1 of 2 operators failed to load — the timeline below is incomplete.');
  });
});

const historyEntry = (id: number, status: string, acked: number): CheckpointHistory =>
  ({
    id,
    status,
    is_savepoint: false,
    trigger_timestamp: TRIGGERED,
    end_to_end_duration: 5,
    num_acknowledged_subtasks: acked,
    num_subtasks: 4,
    state_size: 0
  }) as CheckpointHistory;

describe('JobCheckpointsTimelineComponent polling', () => {
  let component: JobCheckpointsTimelineComponent;
  let applyHistory: (next: CheckpointHistory[]) => void;
  let loads: string[];

  beforeEach(() => {
    component = new JobCheckpointsTimelineComponent(
      {} as JobService,
      { markForCheck: () => undefined } as unknown as ChangeDetectorRef
    );
    loads = [];
    const internals = component as unknown as {
      renderStrip: () => void;
      loadTimeline: (datum: { id: number; rawStatus: string }) => void;
      applyHistory: (next: CheckpointHistory[]) => void;
    };
    // The charts need a real canvas; these tests only check what gets selected and loaded.
    internals.renderStrip = () => undefined;
    internals.loadTimeline = datum => loads.push(`#${datum.id} ${datum.rawStatus}`);
    applyHistory = internals.applyHistory.bind(component);
  });

  it('reloads the followed checkpoint once it completes, without waiting for a newer one', () => {
    applyHistory([historyEntry(7, 'IN_PROGRESS', 2)]);
    applyHistory([historyEntry(7, 'COMPLETED', 4)]);

    expect(loads).toEqual(['#7 IN_PROGRESS', '#7 COMPLETED']);
    expect(component.selected?.acked).toBe('4/4');
  });

  it('does not reload when nothing changed, while a load is in flight, or while pinned', () => {
    applyHistory([historyEntry(7, 'IN_PROGRESS', 2)]);
    applyHistory([historyEntry(7, 'IN_PROGRESS', 2)]);
    component.loadingTimeline = true;
    applyHistory([historyEntry(7, 'IN_PROGRESS', 3)]);
    component.loadingTimeline = false;
    component.userPinned = true;
    applyHistory([historyEntry(7, 'COMPLETED', 4)]);

    expect(loads).toEqual(['#7 IN_PROGRESS']);
  });

  it('bases the empty state on polled history rather than the parent snapshot', () => {
    component.history = [];
    expect(component.hasHistory).toBe(false);

    applyHistory([historyEntry(7, 'COMPLETED', 4)]);

    expect(component.hasHistory).toBe(true);
  });
});

interface LoadingInternals {
  visible$: BehaviorSubject<boolean>;
  detailsCache: Map<number, unknown>;
  stripContainer: ElementRef<HTMLDivElement>;
  timelineContainer: ElementRef<HTMLDivElement>;
  renderStrip: () => void;
  renderTimeline: (datum: { id: number }) => void;
  subscribeTimelineLoads: () => void;
  startPolling: () => void;
  computePollDelayMs: () => number;
  applyHistory: (next: CheckpointHistory[]) => void;
  toStripDatum: (h: CheckpointHistory) => { status: string };
}

describe('JobCheckpointsTimelineComponent loading', () => {
  let component: JobCheckpointsTimelineComponent;
  let internals: LoadingInternals;
  let loadDetails: ReturnType<typeof vi.fn>;
  let loadStats: ReturnType<typeof vi.fn>;
  let rendered: number[];
  const serverError = new HttpErrorResponse({ status: 500 });

  beforeEach(() => {
    loadDetails = vi.fn();
    loadStats = vi.fn();
    component = new JobCheckpointsTimelineComponent(
      { loadCheckpointAllSubtaskDetails: loadDetails, loadCheckpointStats: loadStats } as unknown as JobService,
      { markForCheck: () => undefined } as unknown as ChangeDetectorRef
    );
    component.jobId = 'job-1';
    component.vertices = [vertex('a', 'Agg'), vertex('b', 'Sink')];
    rendered = [];
    internals = component as unknown as LoadingInternals;
    internals.stripContainer = new ElementRef(document.createElement('div'));
    internals.timelineContainer = new ElementRef(document.createElement('div'));
    // The charts need a real canvas; these tests check what gets fetched, cached and rendered.
    internals.renderStrip = () => undefined;
    internals.renderTimeline = datum => rendered.push(datum.id);
    internals.subscribeTimelineLoads();
    internals.visible$.next(true);
  });

  afterEach(() => {
    component.ngOnDestroy();
    vi.useRealTimers();
  });

  it('does not fetch subtask details until the Timeline tab is shown', () => {
    internals.visible$.next(false);
    loadDetails.mockReturnValue(of({ a: details(), b: details() }));

    internals.applyHistory([historyEntry(7, 'COMPLETED', 4)]);
    expect(loadDetails).not.toHaveBeenCalled();

    internals.visible$.next(true);
    expect(loadDetails).toHaveBeenCalledTimes(1);
    expect(rendered).toEqual([7]);
  });

  it('says the JobManager no longer retains the checkpoint when every vertex returns 404', () => {
    loadDetails.mockReturnValue(of({ a: null, b: null }));

    internals.applyHistory([historyEntry(7, 'COMPLETED', 4)]);

    expect(component.timelineEmpty).toBe(true);
    expect(component.loadError).toBe('not-retained');
    expect(component.emptyTimelineMessage).toContain('no longer retains this checkpoint');
  });

  it('reports other request errors as a failed request rather than an evicted checkpoint', () => {
    loadDetails.mockReturnValue(throwError(() => serverError));

    internals.applyHistory([historyEntry(7, 'COMPLETED', 4)]);

    expect(component.loadError).toBe('request-failed');
    expect(component.emptyTimelineMessage).toContain('request to the JobManager failed');
  });

  it('renders a partial load with its failure count but does not cache it', () => {
    loadDetails.mockReturnValue(of({ a: details(), b: null }));

    internals.applyHistory([historyEntry(7, 'COMPLETED', 4)]);

    expect(rendered).toEqual([7]);
    expect([component.failedVertexCount, component.requestedVertexCount]).toEqual([1, 2]);
    expect(internals.detailsCache.has(7)).toBe(false);
  });

  it('drops a load still in flight when the job changes', () => {
    const response = new Subject<Record<string, CheckpointSubTask | null>>();
    loadDetails.mockReturnValue(response);
    internals.applyHistory([historyEntry(42, 'COMPLETED', 4)]);

    component.jobId = 'job-2';
    component.ngOnChanges({ jobId: new SimpleChange('job-1', 'job-2', false) });
    response.next({ a: details(), b: details() });
    response.complete();

    expect(rendered).toEqual([]);
    expect(internals.detailsCache.has(42)).toBe(false);
  });

  it('retries a failed load of the followed checkpoint once rather than on every poll', () => {
    loadDetails.mockReturnValue(throwError(() => serverError));

    for (let poll = 0; poll < 4; poll++) {
      internals.applyHistory([historyEntry(7, 'COMPLETED', 4)]);
    }

    expect(loadDetails).toHaveBeenCalledTimes(2);
  });

  it('draws a failed savepoint as failed, and an in-progress one as a savepoint', () => {
    const savepoint = (status: string): CheckpointHistory => ({ ...historyEntry(7, status, 4), is_savepoint: true });

    expect(internals.toStripDatum(savepoint('FAILED')).status).toBe('FAILED');
    expect(internals.toStripDatum(savepoint('IN_PROGRESS')).status).toBe('SAVEPOINT');
  });

  it.each([
    [undefined, 15_000],
    [0, 15_000],
    [100, 1_500],
    [5_000, 5_000],
    [60_000, 15_000],
    // Long.MAX_VALUE: periodic checkpointing disabled.
    [2 ** 63, 15_000]
  ])('polls every %s ms checkpoint interval at %s ms', (interval, delay) => {
    component.config = { interval } as CheckpointConfig;

    expect(internals.computePollDelayMs()).toBe(delay);
  });

  it('keeps polling after a failed stats request', () => {
    vi.useFakeTimers();
    loadStats
      .mockReturnValueOnce(throwError(() => serverError))
      .mockReturnValue(of({ history: [historyEntry(7, 'COMPLETED', 4)] }));
    loadDetails.mockReturnValue(of({ a: details(), b: details() }));

    internals.startPolling();
    expect(loadStats).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(15_000);

    expect(loadStats).toHaveBeenCalledTimes(2);
    expect(component.selected?.id).toBe(7);
  });
});

describe('buildTimelineTicks', () => {
  it('uses minute steps for checkpoints longer than five minutes', () => {
    expect(buildTimelineTicks(0, 600_000)).toEqual([0, 120_000, 240_000, 360_000, 480_000, 600_000]);
  });

  it('rounds the step up to whole hours past the largest step', () => {
    const hour = 3_600_000;

    expect(buildTimelineTicks(0, 12 * hour)).toEqual([0, 3 * hour, 6 * hour, 9 * hour, 12 * hour]);
  });
});

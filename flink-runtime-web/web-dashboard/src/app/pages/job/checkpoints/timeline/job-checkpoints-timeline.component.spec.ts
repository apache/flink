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
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NEVER, Observable, of, Subject, throwError } from 'rxjs';

import { Checkpoint, CheckpointConfig, CheckpointHistory, CheckpointSubTask } from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { StripDatum } from './checkpoint-timeline-rows';
import { completed, details, historyEntry, noStats, vertex } from './checkpoint-timeline.testing';
import { JobCheckpointsTimelineComponent } from './job-checkpoints-timeline.component';

// jsdom has no canvas, so g2 is swapped for a fake chart that records what the component asks it to draw.
const g2 = vi.hoisted(() => {
  class FakeMark {
    public readonly dataCalls: unknown[][] = [];
    public readonly scales: Array<[string, { domain: unknown[] }]> = [];
    public readonly interactions: Array<[string, unknown]> = [];

    public encode(): this {
      return this;
    }
    public axis(): this {
      return this;
    }
    public legend(): this {
      return this;
    }
    public animate(): this {
      return this;
    }
    public style(): this {
      return this;
    }
    public tooltip(): this {
      return this;
    }
    public data(data: unknown[]): this {
      this.dataCalls.push(data);
      return this;
    }
    public scale(channel: string, options: { domain: unknown[] }): this {
      this.scales.push([channel, options]);
      return this;
    }
    public interaction(name: string, options: unknown): this {
      this.interactions.push([name, options]);
      return this;
    }
  }

  class FakeChart {
    public static readonly instances: FakeChart[] = [];
    public readonly marks: FakeMark[] = [];
    public readonly handlers = new Map<string, (event?: unknown) => void>();
    public readonly emitted: Array<[string, unknown]> = [];
    public readonly attrs = new Map<string, unknown>();
    public readonly interactions: Array<[string, unknown]> = [];
    public renders = 0;
    public destroyed = false;

    constructor(public readonly options: Record<string, unknown>) {
      FakeChart.instances.push(this);
    }

    public interval(): FakeMark {
      const mark = new FakeMark();
      this.marks.push(mark);
      return mark;
    }
    public coordinate(): this {
      return this;
    }
    public interaction(name: string, options: unknown): this {
      this.interactions.push([name, options]);
      return this;
    }
    public on(event: string, handler: (event?: unknown) => void): this {
      this.handlers.set(event, handler);
      return this;
    }
    public emit(event: string, payload?: unknown): void {
      this.emitted.push([event, payload]);
    }
    public attr(key: string, value: unknown): this {
      this.attrs.set(key, value);
      return this;
    }
    public render(): Promise<this> {
      this.renders++;
      return Promise.resolve(this);
    }
    public forceFit(): Promise<void> {
      return Promise.resolve();
    }
    public destroy(): void {
      this.destroyed = true;
    }
  }
  return { FakeChart };
});

vi.mock('@antv/g2', () => ({ Chart: g2.FakeChart }));

type FakeChart = InstanceType<typeof g2.FakeChart>;

describe('JobCheckpointsTimelineComponent', () => {
  let fixture: ComponentFixture<JobCheckpointsTimelineComponent>;
  let element: HTMLElement;
  let loadStats: ReturnType<typeof vi.fn>;
  let loadDetails: ReturnType<typeof vi.fn>;
  // The Timeline tab is shown when the strip has a width; jsdom lays nothing out, so the width is faked.
  let stripWidth: number;
  let resizeCallbacks: Array<() => void>;
  const serverError = new HttpErrorResponse({ status: 500 });

  const strip = (): FakeChart => g2.FakeChart.instances[0];
  const timeline = (): FakeChart | undefined => g2.FakeChart.instances.slice(1).filter(c => !c.destroyed)[0];
  const timelineCharts = (): FakeChart[] => g2.FakeChart.instances.slice(1);
  const stripData = (): StripDatum[] => strip().marks[0].dataCalls.at(-1) as StripDatum[];
  const stripBar = (id: number): StripDatum => stripData().find(d => d.id === id)!;
  const detailsRequests = (): Array<[string, number]> =>
    loadDetails.mock.calls.map(([jobId, id]) => [jobId as string, id as number]);
  const text = (): string => element.textContent!.replace(/\s+/g, ' ');

  const setHistory = (history: CheckpointHistory[]): void => {
    fixture.componentRef.setInput('history', history);
    fixture.detectChanges();
  };
  const mount = (history: CheckpointHistory[], vertexIds = ['a'], config?: CheckpointConfig): void => {
    fixture.componentRef.setInput('jobId', 'job-1');
    fixture.componentRef.setInput(
      'vertices',
      vertexIds.map(id => vertex(id, id === 'a' ? 'Agg' : id === 's' ? 'Sink' : id))
    );
    fixture.componentRef.setInput('config', config);
    setHistory(history);
    // ngAfterViewInit loads the newest checkpoint and starts polling, after the first check of the view.
    fixture.detectChanges();
  };
  const clickBar = (id: number): void => {
    strip().handlers.get('interval:click')!({ data: { data: stripBar(id) } });
    fixture.detectChanges();
  };
  const showTab = (): void => {
    stripWidth = 800;
    resizeCallbacks.forEach(callback => callback());
    fixture.detectChanges();
  };

  beforeEach(async () => {
    g2.FakeChart.instances.length = 0;
    loadStats = vi.fn().mockReturnValue(NEVER);
    loadDetails = vi.fn().mockReturnValue(of({ a: details(completed(0, { async: 5 })) }));
    stripWidth = 800;
    resizeCallbacks = [];
    vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockImplementation(() => stripWidth);
    vi.stubGlobal(
      'ResizeObserver',
      class {
        constructor(callback: () => void) {
          resizeCallbacks.push(callback);
        }
        public observe(): void {}
        public disconnect(): void {}
      }
    );
    vi.stubGlobal('requestAnimationFrame', (callback: FrameRequestCallback) => {
      callback(0);
      return 1;
    });
    await TestBed.configureTestingModule({
      imports: [JobCheckpointsTimelineComponent],
      providers: [
        {
          provide: JobService,
          useValue: { loadCheckpointStats: loadStats, loadCheckpointAllSubtaskDetails: loadDetails }
        }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(JobCheckpointsTimelineComponent);
    element = fixture.nativeElement as HTMLElement;
  });

  afterEach(() => {
    fixture.destroy();
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  describe('strip', () => {
    it('draws the recent checkpoints oldest first in a 160px chart', () => {
      mount([historyEntry(9, 'COMPLETED', 4), historyEntry(8, 'FAILED', 2), historyEntry(7, 'COMPLETED', 4)]);

      expect(strip().options['height']).toBe(160);
      expect(stripData().map(d => [d.id, d.status])).toEqual([
        [7, 'COMPLETED'],
        [8, 'FAILED'],
        [9, 'COMPLETED']
      ]);
      expect(text()).not.toContain('No checkpoint history yet');
    });

    it('says when there is no checkpoint history', () => {
      mount([]);

      expect(text()).toContain('No checkpoint history yet');
      expect(text()).toContain('Per-subtask Timeline');
    });

    it('redraws only when the bars or the pin change', () => {
      mount([historyEntry(7, 'COMPLETED', 4)]);
      const renders = strip().renders;

      setHistory([historyEntry(7, 'COMPLETED', 4)]);
      expect(strip().renders).toBe(renders);
      clickBar(7);
      expect(strip().renders).toBe(renders + 1);
      setHistory([historyEntry(8, 'IN_PROGRESS', 0), historyEntry(7, 'COMPLETED', 4)]);
      expect(strip().renders).toBe(renders + 2);
      // The bars moved, so an open tooltip would point at the wrong one.
      expect(strip().emitted.at(-1)).toEqual(['tooltip:hide', undefined]);
    });

    it('shows the tooltip only while the pointer is over a bar', () => {
      mount([historyEntry(7, 'COMPLETED', 4)]);
      const pointer = (event: string, payload?: unknown): void => strip().handlers.get(event)!(payload);

      expect(strip().marks[0].interactions).toContainEqual(['tooltip', { disableNative: true, wait: 0 }]);
      strip().emitted.length = 0;
      pointer('interval:pointermove', { data: { data: stripBar(7) } });
      pointer('plot:pointermove', { target: { className: 'element' } });
      pointer('interval:pointerout');
      // Over the plot but off the bars, e.g. after a re-render moved them.
      pointer('plot:pointermove', { target: { className: 'plot' } });

      expect(strip().emitted).toEqual([
        ['tooltip:show', { data: { data: stripBar(7) } }],
        ['tooltip:hide', undefined],
        ['tooltip:hide', undefined]
      ]);
    });
  });

  describe('selection', () => {
    it('follows the newest checkpoint', () => {
      mount([historyEntry(8, 'COMPLETED', 4), historyEntry(7, 'COMPLETED', 4)]);

      expect(text()).toContain('Checkpoint #8');
      expect(element.querySelector('.pin-badge')).toBeNull();
      expect(detailsRequests()).toEqual([['job-1', 8]]);
    });

    it('pins a clicked checkpoint until Follow newest is clicked', () => {
      mount([historyEntry(8, 'COMPLETED', 4), historyEntry(7, 'COMPLETED', 4)]);

      clickBar(7);
      expect(text()).toContain('Checkpoint #7');
      expect(element.querySelector('nz-tag.pin-badge')?.textContent?.trim()).toBe('pinned');
      setHistory([historyEntry(9, 'COMPLETED', 4), historyEntry(8, 'COMPLETED', 4), historyEntry(7, 'COMPLETED', 4)]);
      expect(text()).toContain('Checkpoint #7');

      const followNewest = element.querySelector<HTMLButtonElement>('button.follow-newest')!;
      expect(followNewest.classList).toContain('ant-btn');
      followNewest.click();
      fixture.detectChanges();

      expect(text()).toContain('Checkpoint #9');
      expect(element.querySelector('.pin-badge')).toBeNull();
      expect(element.querySelector('button.follow-newest')).toBeNull();
    });

    it('explains when the pinned checkpoint leaves the strip', () => {
      mount([historyEntry(7, 'COMPLETED', 4)]);
      clickBar(7);

      setHistory([historyEntry(9, 'COMPLETED', 4), historyEntry(8, 'COMPLETED', 4)]);

      expect(text()).toContain('Checkpoint #7 is no longer in the recent-checkpoints strip above');
    });

    it('follows the newest checkpoint again once the history empties while pinned', () => {
      mount([historyEntry(7, 'COMPLETED', 4)]);
      clickBar(7);

      setHistory([]);
      expect(text()).toContain('Per-subtask Timeline');
      setHistory([historyEntry(1, 'COMPLETED', 4)]);

      expect(text()).toContain('Checkpoint #1');
      expect(element.querySelector('.pin-badge')).toBeNull();
    });
  });

  describe('reloads', () => {
    it('reloads the followed checkpoint once it completes, without waiting for a newer one', () => {
      mount([historyEntry(7, 'IN_PROGRESS', 2)]);
      setHistory([historyEntry(7, 'COMPLETED', 4)]);

      expect(detailsRequests()).toEqual([
        ['job-1', 7],
        ['job-1', 7]
      ]);
      expect(text()).toContain('acked 4/4');
    });

    it('updates one timeline chart across reloads instead of rebuilding it', () => {
      mount([historyEntry(7, 'IN_PROGRESS', 2)]);
      setHistory([historyEntry(7, 'COMPLETED', 4)]);

      expect(timelineCharts().length).toBe(1);
      expect([timeline()!.renders, timeline()!.destroyed]).toEqual([2, false]);
    });

    it('does not reload when nothing changed or while a load is in flight', () => {
      mount([historyEntry(7, 'IN_PROGRESS', 2)]);
      setHistory([historyEntry(7, 'IN_PROGRESS', 2)]);
      loadDetails.mockReturnValue(NEVER);
      setHistory([historyEntry(7, 'IN_PROGRESS', 3)]);
      setHistory([historyEntry(7, 'IN_PROGRESS', 4)]);

      expect(detailsRequests().length).toBe(2);
    });

    it('keeps refreshing a pinned in-progress checkpoint until it finishes', () => {
      mount([historyEntry(7, 'IN_PROGRESS', 2)]);
      clickBar(7);
      setHistory([historyEntry(8, 'IN_PROGRESS', 0), historyEntry(7, 'IN_PROGRESS', 3)]);
      setHistory([historyEntry(8, 'IN_PROGRESS', 1), historyEntry(7, 'COMPLETED', 4)]);
      setHistory([historyEntry(8, 'COMPLETED', 4), historyEntry(7, 'COMPLETED', 4)]);

      expect(detailsRequests().map(([, id]) => id)).toEqual([7, 7, 7, 7]);
      expect(text()).toContain('Checkpoint #7');
      expect(text()).toContain('acked 4/4');
      expect(element.querySelector('.pin-badge')).not.toBeNull();
    });

    it('re-selects a finished checkpoint from the cache, and fetches an in-progress savepoint again', () => {
      mount([historyEntry(8, 'IN_PROGRESS', 2, true), historyEntry(7, 'COMPLETED', 4, true)]);
      clickBar(7);
      clickBar(8);
      clickBar(7);
      clickBar(8);

      expect(detailsRequests().map(([, id]) => id)).toEqual([8, 7, 8, 8]);
    });

    it('keeps cached details only for checkpoints still in the strip, plus the pinned one', () => {
      // The cache isn't visible in the page, so this one reads it.
      const cached = (): number[] =>
        [...(fixture.componentInstance as unknown as { detailsCache: Map<number, unknown> }).detailsCache.keys()].sort(
          (a, b) => a - b
        );
      mount([historyEntry(7, 'COMPLETED', 4)]);
      clickBar(7);
      setHistory([historyEntry(9, 'COMPLETED', 4), historyEntry(8, 'COMPLETED', 4)]);
      expect(cached()).toEqual([7]);

      element.querySelector<HTMLButtonElement>('button.follow-newest')!.click();
      setHistory([historyEntry(10, 'COMPLETED', 4), historyEntry(9, 'COMPLETED', 4)]);
      expect(cached()).toEqual([9, 10]);
    });
  });

  describe('loading', () => {
    it('does not fetch subtask details until the Timeline tab is shown', () => {
      stripWidth = 0;
      mount([historyEntry(7, 'COMPLETED', 4)]);
      expect(loadDetails).not.toHaveBeenCalled();
      expect(loadStats).not.toHaveBeenCalled();

      showTab();

      expect(detailsRequests()).toEqual([['job-1', 7]]);
      expect(loadStats).toHaveBeenCalledWith('job-1');
      expect(timeline()).toBeDefined();
    });

    it('says the JobManager no longer retains the checkpoint when every vertex returns 404', () => {
      loadDetails.mockReturnValue(of({ a: null }));
      mount([historyEntry(7, 'COMPLETED', 4)]);

      expect(text()).toContain('the JobManager no longer retains this checkpoint');
      expect(timeline()).toBeUndefined();
    });

    it('reports other request errors as a failed request, and retries once rather than on every poll', () => {
      loadDetails.mockReturnValue(throwError(() => serverError));
      mount([historyEntry(7, 'COMPLETED', 4)]);
      expect(text()).toContain('the request to the JobManager failed');

      for (let poll = 0; poll < 3; poll++) {
        setHistory([historyEntry(7, 'COMPLETED', 4)]);
      }

      expect(loadDetails).toHaveBeenCalledTimes(2);
    });

    it('draws a partial load with a warning, and does not cache it', () => {
      loadDetails.mockReturnValue(of({ a: details(completed(0, { async: 5 })), b: null }));
      mount([historyEntry(7, 'COMPLETED', 4)], ['a', 'b']);

      expect(text()).toContain('1 of 2 operators failed to load — the timeline below is incomplete.');
      expect(timeline()!.marks[0].dataCalls.at(-1)).toHaveLength(1);
      clickBar(7);
      expect(loadDetails).toHaveBeenCalledTimes(2);
    });

    it('drops a load still in flight when the job changes', () => {
      const response = new Subject<Record<string, CheckpointSubTask | null>>();
      loadDetails.mockReturnValue(response);
      mount([historyEntry(42, 'COMPLETED', 4)]);

      fixture.componentRef.setInput('jobId', 'job-2');
      fixture.detectChanges();
      response.next({ a: details(completed(0, { async: 5 })) });
      expect(timeline()).toBeUndefined();

      loadDetails.mockReturnValue(of({ a: details(completed(0, { async: 5 })) }));
      setHistory([historyEntry(42, 'COMPLETED', 4)]);
      expect(detailsRequests()).toEqual([
        ['job-1', 42],
        ['job-2', 42]
      ]);
    });
  });

  describe('polling', () => {
    const stats = (history: CheckpointHistory[]): Observable<Checkpoint> => of({ history } as Checkpoint);

    it('shows polled history before the parent passes any', () => {
      loadStats.mockReturnValue(stats([historyEntry(7, 'COMPLETED', 4)]));
      mount([]);

      expect(text()).not.toContain('No checkpoint history yet');
      expect(text()).toContain('Checkpoint #7');
    });

    it('drops a stats poll for the previous job that lands after the job changes', () => {
      const oldJobStats = new Subject<Checkpoint>();
      loadStats.mockReturnValue(oldJobStats);
      mount([]);

      fixture.componentRef.setInput('jobId', 'job-2');
      fixture.detectChanges();
      oldJobStats.next({ history: [historyEntry(42, 'COMPLETED', 4)] } as Checkpoint);
      fixture.detectChanges();

      expect(loadStats).toHaveBeenCalledWith('job-1');
      expect(text()).toContain('No checkpoint history yet');
      expect(loadDetails).not.toHaveBeenCalled();
    });

    it.each([
      [undefined, 15_000],
      [0, 15_000],
      [100, 1_500],
      [5_000, 5_000],
      [60_000, 15_000],
      // Long.MAX_VALUE: periodic checkpointing disabled.
      [2 ** 63, 15_000]
    ])('polls a job with a %s ms checkpoint interval every %s ms', (interval, delay) => {
      vi.useFakeTimers();
      loadStats.mockReturnValue(stats([historyEntry(7, 'COMPLETED', 4)]));
      mount([], ['a'], { interval } as CheckpointConfig);
      expect(loadStats).toHaveBeenCalledTimes(1);

      vi.advanceTimersByTime(delay - 1);
      expect(loadStats).toHaveBeenCalledTimes(1);
      vi.advanceTimersByTime(1);
      expect(loadStats).toHaveBeenCalledTimes(2);
    });

    it('keeps the last stats on a failed poll, marked as stale, and keeps polling', () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date(2026, 0, 1, 10, 20, 30));
      loadStats
        .mockReturnValueOnce(throwError(() => serverError))
        .mockReturnValue(stats([historyEntry(8, 'COMPLETED', 4), historyEntry(7, 'COMPLETED', 4)]));
      mount([historyEntry(7, 'COMPLETED', 4)]);

      expect(text()).toContain('Checkpoint #7');
      expect(element.querySelector('nz-alert.stale-stats')?.textContent).toContain(
        'Could not refresh the checkpoint statistics from the JobManager; showing the ones loaded at 10:20:30. Retrying.'
      );

      vi.advanceTimersByTime(15_000);
      fixture.detectChanges();

      expect(element.querySelector('nz-alert.stale-stats')).toBeNull();
      expect(text()).toContain('Checkpoint #8');
    });
  });

  describe('row window', () => {
    let nextId = 1;
    const phases = (): FakeChart['marks'][number] => timeline()!.marks[0];
    const lastScale = (channel: string): { domain: unknown[] } =>
      phases()
        .scales.filter(([c]) => c === channel)
        .at(-1)![1];
    const shownRows = (): unknown[] => lastScale('x').domain;
    const scrollBox = (): HTMLDivElement => element.querySelector<HTMLDivElement>('.timeline-scroll')!;
    // A checkpoint of one operator where a higher subtask index finishes later: the slowest 25 of 300 are #275–#299.
    const showSubtasks = (count: number): void => {
      loadDetails.mockReturnValue(
        of({ a: details(...Array.from({ length: count }, (_, i) => completed(i, { async: 5 + i }))) })
      );
      setHistory([historyEntry(nextId++, 'COMPLETED', 4)]);
    };
    const scrollTo = (row: number): void => {
      Object.defineProperty(scrollBox(), 'scrollTop', { value: row * 22, writable: true, configurable: true });
      scrollBox().dispatchEvent(new Event('scroll'));
      fixture.detectChanges();
    };
    const toggleSlowestFirst = (): void => {
      element.querySelector<HTMLLabelElement>('label.slowest-first')!.click();
      fixture.detectChanges();
    };
    const pickSlowest = (count: number): void => {
      fixture.componentInstance.setSlowestCount(count);
      fixture.detectChanges();
    };

    beforeEach(() => mount([]));

    it('shows the 25 slowest subtasks in operator order, and offers up to 200 or the row count', async () => {
      showSubtasks(300);
      // ngModel writes the select's value asynchronously.
      await fixture.whenStable();
      fixture.detectChanges();

      expect(shownRows()).toEqual(Array.from({ length: 25 }, (_, i) => `Agg #${275 + i}`));
      expect(new Set((phases().dataCalls.at(-1) as Array<{ row: string }>).map(r => r.row)).size).toBe(25);
      expect(timeline()!.attrs.get('height')).toBe(25 * 22 + 56);
      expect(text()).toContain('Showing the 25 slowest of 300 subtasks');
      expect(element.querySelector('nz-select.row-count')).not.toBeNull();
      expect(scrollBox().classList).not.toContain('scrollable');
      expect(fixture.componentInstance.slowestCountOptions).toEqual([25, 50, 75, 100, 125, 150, 175, 200]);

      showSubtasks(60);
      expect(fixture.componentInstance.slowestCountOptions).toEqual([25, 50, 60]);

      showSubtasks(20);
      expect(element.querySelector('nz-select.row-count')).toBeNull();
      expect(timeline()!.attrs.get('height')).toBe(20 * 22 + 56);
    });

    it('scrolls through more of the slowest than fit, without moving the time scale', () => {
      showSubtasks(300);
      const timeDomain = lastScale('y').domain;

      pickSlowest(100);
      expect(scrollBox().classList).toContain('scrollable');
      expect(scrollBox().style.height).toBe(`${25 * 22 + 56}px`);
      expect([shownRows()[0], text().includes('rows 1–25')]).toEqual(['Agg #200', true]);
      scrollTo(30);

      expect([shownRows()[0], shownRows().length, text().includes('rows 31–55')]).toEqual(['Agg #230', 25, true]);
      expect(lastScale('y').domain).toEqual(timeDomain);
    });

    it('keeps the window within the picked rows, and starts from the top for another count', () => {
      showSubtasks(300);
      pickSlowest(100);
      scrollTo(90);
      expect(text()).toContain('rows 76–100');

      pickSlowest(50);

      expect([scrollBox().scrollTop, shownRows()[0]]).toEqual([0, 'Agg #250']);
      expect(text()).toContain('rows 1–25');
    });

    it('sorts the picked rows by when they finished, latest first, and starts again from the top', () => {
      showSubtasks(300);
      pickSlowest(50);
      scrollTo(10);

      toggleSlowestFirst();

      expect(scrollBox().scrollTop).toBe(0);
      expect(shownRows().slice(0, 3)).toEqual(['Agg #299', 'Agg #298', 'Agg #297']);
      toggleSlowestFirst();
      expect(shownRows()[0]).toBe('Agg #250');
    });

    it('puts subtasks without an ack first when sorting, keeping operator order for ties', () => {
      fixture.componentRef.setInput('vertices', [vertex('a', 'Agg'), vertex('s', 'Sink')]);
      loadDetails.mockReturnValue(
        of({
          a: details(completed(0, { async: 10 }), noStats(1), completed(2, { async: 30 })),
          s: details(noStats(0), completed(1, { async: 30 }))
        })
      );
      setHistory([historyEntry(nextId++, 'FAILED', 3)]);

      toggleSlowestFirst();

      expect(shownRows()).toEqual(['Agg #1', 'Sink #0', 'Agg #2', 'Sink #1', 'Agg #0']);
    });

    it('counts aborted subtasks as the slowest, however short their reported phases', () => {
      // A failed checkpoint: 30 subtasks finished, #30 was aborted after reporting 1ms, #31 never acked.
      loadDetails.mockReturnValue(
        of({
          a: details(
            ...Array.from({ length: 30 }, (_, i) => completed(i, { async: 5 + i })),
            completed(30, { async: 1 }, true),
            noStats(31)
          )
        })
      );
      setHistory([historyEntry(nextId++, 'FAILED', 30)]);

      expect(shownRows()).toContain('Agg #30');
      expect(shownRows()).toContain('Agg #31');
      toggleSlowestFirst();
      expect(shownRows().slice(0, 3)).toEqual(['Agg #30', 'Agg #31', 'Agg #29']);
    });
  });

  it('highlights the hovered row across the timeline', () => {
    mount([historyEntry(7, 'COMPLETED', 4)]);

    expect(timeline()!.interactions).toContainEqual(['elementHighlightByX', true]);
  });

  it('shows a timeline tooltip only over a bar, next to the pointer, and hides it when the rows are redrawn', () => {
    mount([historyEntry(7, 'COMPLETED', 4)]);
    const chart = timeline()!;
    const pointer = (event: string, payload?: unknown): void => chart.handlers.get(event)!(payload);
    const bar = { phase: 'async' };
    const plot = { className: 'plot', getBBox: () => ({ x: 50, y: 20 }) };

    expect(chart.interactions).toContainEqual(['tooltip', expect.objectContaining({ disableNative: true, wait: 0 })]);
    chart.emitted.length = 0;
    pointer('interval:pointermove', {
      data: { data: bar },
      canvas: { x: 80, y: 30 },
      target: { className: 'element', parentNode: { className: 'group', parentNode: plot } }
    });
    pointer('plot:pointermove', { target: { className: 'plot' } });
    clickBar(7);

    expect(chart.emitted).toEqual([
      // g2 takes the position relative to the plot area.
      ['tooltip:show', { offsetX: 30, offsetY: 10, data: { data: bar } }],
      ['tooltip:hide', undefined],
      ['tooltip:hide', undefined]
    ]);
  });
});

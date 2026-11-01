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

import { DatePipe, NgIf } from '@angular/common';
import {
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnDestroy,
  SimpleChanges,
  ViewChild
} from '@angular/core';
import { BehaviorSubject, defer, EMPTY, Observable, of, Subject, timer } from 'rxjs';
import { catchError, distinctUntilChanged, filter, map, repeat, switchMap, takeUntil } from 'rxjs/operators';

import { Chart } from '@antv/g2';
import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import {
  CheckpointConfig,
  CheckpointHistory,
  CheckpointSubTask,
  CompletedSubTaskCheckpointStatistics,
  SubTaskCheckpointStatisticsItem,
  VerticesItem
} from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTooltipModule } from 'ng-zorro-antd/tooltip';

type PhaseKey = 'start_delay' | 'alignment' | 'sync' | 'async';

// 'not-retained': every vertex returned 404, i.e. the JobManager no longer keeps this checkpoint's stats.
type LoadError = 'not-retained' | 'request-failed';

interface TimelineLoad {
  datum: StripDatum;
  perVertex: Record<string, CheckpointSubTask | null>;
  failedCount: number;
  requestedCount: number;
  error?: LoadError;
}

interface StripDatum {
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

interface TimelineDatum {
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
interface UnackedDatum {
  row: string;
  operatorFull: string;
  subtask: number;
  label: 'no ack' | 'pending';
  range: [number, number];
}

// A subtask whose phases all rounded to 0ms, drawn as a hairline at trigger time so its row isn't empty.
interface InstantDatum {
  row: string;
  operatorFull: string;
  subtask: number;
  aborted: boolean;
  stateSize: number;
  checkpointedSize: number;
  range: [number, number];
}

const STRIP_COLOR: Record<StripDatum['status'], string> = {
  COMPLETED: '#52c41a',
  SAVEPOINT: '#722ed1',
  IN_PROGRESS: '#faad14',
  FAILED: '#f5222d'
};

const STRIP_STATUSES = Object.keys(STRIP_COLOR) as Array<StripDatum['status']>;

const PHASE_COLOR: Record<PhaseKey, string> = {
  start_delay: '#b8c0cc',
  alignment: '#91caff',
  sync: '#fa8c16',
  async: '#52c41a'
};

const PHASES = Object.keys(PHASE_COLOR) as PhaseKey[];

const NOT_COMPLETED_STROKE = '#f5222d';
const NO_ACK_FILL = '#fff1f0';
const NO_ACK_STRIPE = '#ffa39e';
const INSTANT_FILL = '#8c8c8c';

const OPERATOR_NAME_MAX = 60;

// Local HH:MM:SS.mmm — matches the header's DatePipe default (browser timezone).
function formatWallClock(epochMs: number): string {
  const d = new Date(epochMs);
  const pad = (n: number, len = 2): string => String(n).padStart(len, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

const MINUTE_MS = 60_000;
const HOUR_MS = 60 * MINUTE_MS;
const TICK_STEPS_MS = [
  ...[1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000],
  ...[1, 2, 5, 10, 30, 60].map(minutes => minutes * MINUTE_MS)
];

export function buildTimelineTicks(min: number, max: number, target = 5): number[] {
  const span = Math.max(max - min, 1);
  const desired = span / target;
  const step = TICK_STEPS_MS.find(s => s >= desired) ?? Math.ceil(desired / HOUR_MS) * HOUR_MS;
  const ticks: number[] = [];
  for (let t = min; t <= max + 0.5; t += step) {
    ticks.push(t);
  }
  if (ticks[ticks.length - 1] < max) {
    ticks.push(max);
  }
  return ticks;
}

@Component({
  selector: 'flink-job-checkpoints-timeline',
  templateUrl: './job-checkpoints-timeline.component.html',
  styleUrls: ['./job-checkpoints-timeline.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NgIf,
    DatePipe,
    HumanizeDurationPipe,
    HumanizeBytesPipe,
    NzAlertModule,
    NzEmptyModule,
    NzSpinModule,
    NzTooltipModule
  ]
})
export class JobCheckpointsTimelineComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() public jobId: string;
  @Input() public vertices: VerticesItem[] = [];
  @Input() public history: CheckpointHistory[] = [];
  @Input() public config?: CheckpointConfig;

  @ViewChild('stripContainer', { static: true }) private readonly stripContainer: ElementRef<HTMLDivElement>;
  @ViewChild('timelineContainer', { static: true }) private readonly timelineContainer: ElementRef<HTMLDivElement>;

  public selected?: StripDatum;
  public loadingTimeline = false;
  public timelineEmpty = false;
  public loadError?: LoadError;
  public failedVertexCount = 0;
  public requestedVertexCount = 0;

  // Diverges from @Input once polling starts feeding live updates.
  private currentHistory: CheckpointHistory[] = [];
  public userPinned = false;
  private stripChart?: Chart;
  private stripInterval?: ReturnType<Chart['interval']>;
  private timelineChart?: Chart;
  private noAckHatch?: HTMLCanvasElement;
  private resizeObserver?: ResizeObserver;
  private readonly destroy$ = new Subject<void>();
  // null cancels a load in flight.
  private readonly loadTimeline$ = new Subject<StripDatum | null>();
  // A load requested while the Timeline tab was hidden; it runs once the tab is shown.
  private loadDeferred = false;
  // The checkpoint whose failed load was already retried while following, so polls don't retry it again.
  private retriedCheckpointId?: number;
  // Terminal (completed or failed) checkpoint stats no longer change; memoize by id for instant re-selects.
  private readonly detailsCache = new Map<number, Record<string, CheckpointSubTask | null>>();
  private readonly visible$ = new BehaviorSubject<boolean>(false);
  private static readonly POLL_MIN_MS = 1500;
  private static readonly POLL_MAX_MS = 15000;
  private static readonly STRIP_MAX_BARS = 60;
  private static readonly DETAILS_CACHE_MAX = 100;
  private static readonly TIMELINE_ROW_HEIGHT = 22;
  private static readonly TIMELINE_HEIGHT_BASE = 80;
  private static readonly TIMELINE_HEIGHT_MIN = 160;
  private static readonly LABEL_CHAR_WIDTH = 7;
  private static readonly LABEL_PADDING_BUFFER = 24;
  private static readonly LABEL_PADDING_MIN = 260;
  private static readonly LABEL_PADDING_MAX = 520;

  private readonly humanizeBytesPipe = new HumanizeBytesPipe();
  private readonly humanizeDurationPipe = new HumanizeDurationPipe();

  constructor(
    private readonly jobService: JobService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngAfterViewInit(): void {
    this.currentHistory = this.history || [];
    this.renderStrip();
    this.subscribeTimelineLoads();
    if (this.currentHistory.length > 0) {
      this.selectInternal(this.toStripDatum(this.currentHistory[0]), false);
    }
    this.startPolling();
    // G2 locks autoFit at construction; re-fit + resume polling once width is real.
    this.visible$.next(this.stripContainer.nativeElement.clientWidth > 0);
    if (typeof ResizeObserver !== 'undefined') {
      this.resizeObserver = new ResizeObserver(() => {
        const stripVisible = this.stripContainer.nativeElement.clientWidth > 0;
        if (this.stripChart && stripVisible) {
          void this.stripChart.forceFit();
        }
        if (this.timelineChart && this.timelineContainer.nativeElement.clientWidth > 0) {
          void this.timelineChart.forceFit();
        }
        this.visible$.next(stripVisible);
      });
      this.resizeObserver.observe(this.stripContainer.nativeElement);
      this.resizeObserver.observe(this.timelineContainer.nativeElement);
    }
  }

  public ngOnChanges(changes: SimpleChanges): void {
    if (!this.stripContainer || !this.stripContainer.nativeElement) {
      return;
    }
    if (changes['jobId'] && !changes['jobId'].firstChange) {
      this.userPinned = false;
      this.selected = undefined;
      this.currentHistory = [];
      // Otherwise a load still in flight for the old job lands in the cache under an id the new job reuses.
      this.loadTimeline$.next(null);
      this.detailsCache.clear();
      this.retriedCheckpointId = undefined;
      this.clearTimeline();
      this.renderStrip();
    }
    if (changes['history']) {
      this.applyHistory(this.history || []);
    }
    if (changes['vertices'] && !changes['vertices'].firstChange && this.selected) {
      this.loadTimeline(this.selected);
    }
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.resizeObserver?.disconnect();
    this.stripChart?.destroy();
    this.timelineChart?.destroy();
  }

  private toStripDatum(h: CheckpointHistory): StripDatum {
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

  private renderStrip(): void {
    const data = (this.currentHistory || [])
      .slice(0, JobCheckpointsTimelineComponent.STRIP_MAX_BARS)
      .map(h => this.toStripDatum(h))
      .reverse();

    if (!this.stripChart || !this.stripInterval) {
      this.stripChart = new Chart({
        container: this.stripContainer.nativeElement,
        autoFit: true,
        height: 110,
        paddingTop: 16,
        paddingRight: 16,
        paddingBottom: 36,
        paddingLeft: 56
      });
      const isPinned = (d: StripDatum): boolean => this.userPinned && this.selected?.id === d.id;
      this.stripInterval = this.stripChart
        .interval()
        .encode('x', 'idLabel')
        .encode('y', 'duration')
        .encode('color', 'status')
        .scale('y', { nice: true, domainMin: 0 })
        .scale('color', { domain: STRIP_STATUSES, range: STRIP_STATUSES.map(s => STRIP_COLOR[s]) })
        .axis('x', { title: false, labelAutoHide: true, labelAutoRotate: false })
        .axis('y', { title: false })
        .legend(false)
        .animate(false)
        .style('maxWidth', 18)
        // Keeps 0ms bars (e.g. in progress before the first ack) and very short ones visible and clickable.
        .style('minHeight', 3)
        .style('stroke', (d: StripDatum) => (isPinned(d) ? '#1890ff' : 'transparent'))
        .style('lineWidth', (d: StripDatum) => (isPinned(d) ? 2.5 : 0))
        .tooltip({
          title: '',
          items: [
            (d: StripDatum) => ({
              name: `#${d.idLabel}`,
              color: STRIP_COLOR[d.status],
              value: `${d.rawStatus} · ${this.humanizeDurationPipe.transform(d.duration)} · acked ${d.acked}`
            })
          ]
        });
      this.stripChart.on('interval:click', (ev: { data?: { data?: StripDatum } }) => {
        const datum = ev.data?.data;
        if (datum) {
          this.select(datum);
        }
      });
    }

    this.stripInterval.scale('x', { domain: data.map(d => d.idLabel) });
    this.stripInterval.data(data);
    void this.stripChart.render();
  }

  public select(datum: StripDatum): void {
    // Clicking the already-pinned bar resumes auto-follow.
    if (this.userPinned && this.selected && this.selected.id === datum.id) {
      this.followNewest();
      return;
    }
    this.selectInternal(datum, true);
  }

  public followNewest(): void {
    this.userPinned = false;
    if (this.currentHistory.length > 0) {
      this.selectInternal(this.toStripDatum(this.currentHistory[0]), false);
    } else {
      this.cdr.markForCheck();
    }
  }

  private selectInternal(datum: StripDatum, pinned: boolean): void {
    this.selected = datum;
    if (pinned) {
      this.userPinned = true;
    }
    this.cdr.markForCheck();
    this.renderStrip();
    this.loadTimeline(datum);
  }

  private applyHistory(next: CheckpointHistory[]): void {
    const prevNewestId = this.currentHistory.length > 0 ? this.currentHistory[0].id : -1;
    const nextNewestId = next.length > 0 ? next[0].id : -1;
    this.currentHistory = next;
    this.renderStrip();
    // The empty state and the rotated-out note read currentHistory.
    this.cdr.markForCheck();

    if (next.length === 0) {
      this.selected = undefined;
      this.clearTimeline();
      return;
    }

    if (this.userPinned) {
      return;
    }

    const newest = this.toStripDatum(next[0]);
    if (nextNewestId !== prevNewestId) {
      this.selectInternal(newest, false);
      return;
    }
    // Still the same newest checkpoint: reload once it gained acks or finished, or retry a failed load once
    // (retrying on every poll would repeat the error notifications). Not while a load is in flight, so polling
    // doesn't keep cancelling a slow fetch.
    const selected = this.selected;
    if (selected?.id !== newest.id || this.loadingTimeline) {
      return;
    }
    const progressed = newest.rawStatus !== selected.rawStatus || newest.acked !== selected.acked;
    const retry =
      (this.loadError !== undefined || this.failedVertexCount > 0) && this.retriedCheckpointId !== newest.id;
    if (retry) {
      this.retriedCheckpointId = newest.id;
    }
    if (progressed || retry) {
      this.selectInternal(newest, false);
    }
  }

  private startPolling(): void {
    this.visible$
      .pipe(
        distinctUntilChanged(),
        switchMap(visible =>
          visible
            ? defer(() =>
                this.jobId
                  ? this.jobService.loadCheckpointStats(this.jobId).pipe(catchError(() => of(undefined)))
                  : of(undefined)
              ).pipe(repeat({ delay: () => timer(this.computePollDelayMs()) }))
            : EMPTY
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(stats => {
        if (!stats) {
          return;
        }
        this.applyHistory(stats.history || []);
      });
  }

  private computePollDelayMs(): number {
    // Disabled periodic checkpointing reports Long.MAX_VALUE, which the upper clamp covers.
    const interval = this.config?.interval ?? 0;
    return interval > 0
      ? Math.max(
          JobCheckpointsTimelineComponent.POLL_MIN_MS,
          Math.min(interval, JobCheckpointsTimelineComponent.POLL_MAX_MS)
        )
      : JobCheckpointsTimelineComponent.POLL_MAX_MS;
  }

  private loadTimeline(datum: StripDatum): void {
    // The component also lives in the hidden tab; don't fetch every vertex's details until it is shown.
    if (!this.visible$.value) {
      this.loadDeferred = true;
      return;
    }
    this.loadDeferred = false;
    if (!this.jobId || this.vertices.length === 0) {
      this.showEmptyTimeline();
      return;
    }
    this.loadingTimeline = true;
    this.timelineEmpty = false;
    this.loadError = undefined;
    this.failedVertexCount = 0;
    this.cdr.markForCheck();
    this.loadTimeline$.next(datum);
  }

  private cacheDetails(datum: StripDatum, perVertex: Record<string, CheckpointSubTask | null>): void {
    // In-progress stats still change; only cache terminal checkpoints. `status` is 'SAVEPOINT' for an in-progress
    // savepoint, so check rawStatus.
    if (datum.rawStatus === 'IN_PROGRESS') {
      return;
    }
    this.detailsCache.set(datum.id, perVertex);
    // Evict oldest (Map keeps insertion order).
    while (this.detailsCache.size > JobCheckpointsTimelineComponent.DETAILS_CACHE_MAX) {
      const oldest = this.detailsCache.keys().next().value;
      if (oldest === undefined) {
        break;
      }
      this.detailsCache.delete(oldest);
    }
  }

  private subscribeTimelineLoads(): void {
    this.visible$
      .pipe(
        filter(visible => visible && this.loadDeferred),
        takeUntil(this.destroy$)
      )
      .subscribe(() => {
        if (this.selected) {
          this.loadTimeline(this.selected);
        }
      });

    this.loadTimeline$
      .pipe(
        switchMap((datum): Observable<TimelineLoad> => {
          if (!datum) {
            return EMPTY;
          }
          const requestedCount = this.vertices.length;
          const cached = this.detailsCache.get(datum.id);
          if (cached) {
            return of({ datum, perVertex: cached, failedCount: 0, requestedCount });
          }
          const vertexIds = this.vertices.map(v => v.id);
          return this.jobService.loadCheckpointAllSubtaskDetails(this.jobId, datum.id, vertexIds).pipe(
            map(perVertex => {
              const failedCount = requestedCount - Object.values(perVertex).filter(v => v !== null).length;
              if (failedCount === 0) {
                this.cacheDetails(datum, perVertex);
              }
              const error: LoadError | undefined =
                requestedCount > 0 && failedCount === requestedCount ? 'not-retained' : undefined;
              return { datum, perVertex, failedCount, requestedCount, error };
            }),
            catchError(() =>
              of<TimelineLoad>({
                datum,
                perVertex: {},
                failedCount: requestedCount,
                requestedCount,
                error: 'request-failed'
              })
            )
          );
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(({ datum, perVertex, failedCount, requestedCount, error }) => {
        this.loadingTimeline = false;
        if (error) {
          this.showEmptyTimeline(error);
          return;
        }
        this.failedVertexCount = failedCount;
        this.requestedVertexCount = requestedCount;
        this.renderTimeline(datum, perVertex);
      });
  }

  private buildTimelineRows(
    datum: StripDatum,
    perVertex: Record<string, CheckpointSubTask | null>
  ): { rows: TimelineDatum[]; unacked: UnackedDatum[]; instants: InstantDatum[]; rowOrder: string[] } {
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

    for (const vertex of this.vertices) {
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

      for (const s of cp.subtasks) {
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
        const unaligned = !!s.unaligned_checkpoint;
        // In an unaligned checkpoint, async already includes the alignment (checkpoint_monitoring.md), so don't stack it.
        const alignment = unaligned ? 0 : reportedAlignment;
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
              phaseLabel: phase === 'async' && unaligned ? `${phase} (incl. ${reportedAlignment}ms alignment)` : phase,
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

  private renderTimeline(datum: StripDatum, perVertex: Record<string, CheckpointSubTask | null>): void {
    const { rows, unacked, instants, rowOrder } = this.buildTimelineRows(datum, perVertex);
    if (rowOrder.length === 0) {
      this.showEmptyTimeline();
      return;
    }
    this.timelineEmpty = false;
    this.cdr.markForCheck();

    const C = JobCheckpointsTimelineComponent;
    const chartHeight = Math.max(
      rowOrder.length * C.TIMELINE_ROW_HEIGHT + C.TIMELINE_HEIGHT_BASE,
      C.TIMELINE_HEIGHT_MIN
    );
    // Pad left to the longest label so G2 won't re-clip it.
    const longestLabel = rowOrder.reduce((m, s) => Math.max(m, s.length), 0);
    const leftPadding = Math.min(
      C.LABEL_PADDING_MAX,
      Math.max(C.LABEL_PADDING_MIN, longestLabel * C.LABEL_CHAR_WIDTH + C.LABEL_PADDING_BUFFER)
    );

    const container = this.timelineContainer.nativeElement;
    this.timelineChart?.destroy();
    this.timelineChart = new Chart({
      container,
      autoFit: true,
      height: chartHeight,
      paddingTop: 24,
      paddingRight: 32,
      paddingBottom: 48,
      paddingLeft: leftPadding
    });
    this.timelineChart.coordinate({ transform: [{ type: 'transpose' }] });
    const scaleMin = datum.triggered;
    const scaleMax = Math.max(
      [...rows, ...unacked].reduce((m, r) => Math.max(m, r.range[1]), scaleMin),
      scaleMin + 1
    );
    const xScale = { domain: rowOrder };
    const yScale = {
      domain: [scaleMin, scaleMax],
      nice: false,
      tickMethod: () => buildTimelineTicks(scaleMin, scaleMax)
    };
    this.timelineChart
      .interval()
      .data(rows)
      .encode('x', 'row')
      .encode('y', 'range')
      .encode('color', 'phase')
      .scale('x', xScale)
      .scale('y', yScale)
      .scale('color', { domain: PHASES, range: PHASES.map(p => PHASE_COLOR[p]) })
      .axis('x', {
        title: false,
        labelAutoHide: false,
        labelAutoRotate: false,
        labelFill: '#262626',
        labelOpacity: 1
      })
      .axis('y', { title: 'time', labelFormatter: (v: number) => formatWallClock(Number(v)) })
      .legend(false)
      .animate(false)
      .style('stroke', (d: TimelineDatum) => (d.aborted ? NOT_COMPLETED_STROKE : 'transparent'))
      .style('lineWidth', (d: TimelineDatum) => (d.aborted ? 1.5 : 0))
      .style('lineDash', (d: TimelineDatum) => (d.aborted ? [3, 2] : [0, 0]))
      .tooltip({
        title: (d: TimelineDatum) => `${d.operatorFull} #${d.subtask}`,
        items: [
          (d: TimelineDatum) => ({ name: d.phaseLabel, color: PHASE_COLOR[d.phase], value: `${d.durationMs}ms` }),
          (d: TimelineDatum) => ({ name: 'total', color: 'transparent', value: `${d.totalDuration}ms` }),
          (d: TimelineDatum) => ({
            name: 'state',
            color: 'transparent',
            value: this.humanizeBytesPipe.transform(d.stateSize)
          }),
          (d: TimelineDatum) => ({
            name: 'checkpointed',
            color: 'transparent',
            value: this.humanizeBytesPipe.transform(d.checkpointedSize)
          })
        ]
      })
      // g2 5 ellipsizes the tooltip title at 360px; let full operator names wrap instead.
      .interaction('tooltip', {
        css: {
          '.g2-tooltip': { 'max-width': '480px' },
          '.g2-tooltip-title': { 'white-space': 'normal', 'overflow-wrap': 'anywhere' }
        }
      });
    if (unacked.length > 0) {
      // Returned from a callback: g2 deep-copies constant style objects, which would turn the canvas into a plain object.
      const hatchFill = { image: (this.noAckHatch ??= createHatchPattern()), repetition: 'repeat' };
      this.timelineChart
        .interval()
        .data(unacked)
        .encode('x', 'row')
        .encode('y', 'range')
        .scale('x', xScale)
        .scale('y', yScale)
        .animate(false)
        .style('fill', () => hatchFill)
        .style('stroke', NOT_COMPLETED_STROKE)
        .style('lineWidth', 1.5)
        .style('lineDash', [3, 2])
        .tooltip({
          title: (d: UnackedDatum) => `${d.operatorFull} #${d.subtask}`,
          items: [(d: UnackedDatum) => ({ name: d.label, color: NOT_COMPLETED_STROKE, value: 'no stats reported' })]
        });
    }
    if (instants.length > 0) {
      this.timelineChart
        .interval()
        .data(instants)
        .encode('x', 'row')
        .encode('y', 'range')
        .scale('x', xScale)
        .scale('y', yScale)
        .animate(false)
        // The range is zero-length; once transposed, minHeight is the on-screen length along the time axis.
        .style('minHeight', 2)
        .style('fill', (d: InstantDatum) => (d.aborted ? NOT_COMPLETED_STROKE : INSTANT_FILL))
        .tooltip({
          title: (d: InstantDatum) => `${d.operatorFull} #${d.subtask}`,
          // Same rows as the phase bars, so 'total' always means the sum of the phases.
          items: [
            (d: InstantDatum) => ({
              name: 'all phases',
              color: d.aborted ? NOT_COMPLETED_STROKE : INSTANT_FILL,
              value: '0ms'
            }),
            () => ({ name: 'total', color: 'transparent', value: '0ms' }),
            (d: InstantDatum) => ({
              name: 'state',
              color: 'transparent',
              value: this.humanizeBytesPipe.transform(d.stateSize)
            }),
            (d: InstantDatum) => ({
              name: 'checkpointed',
              color: 'transparent',
              value: this.humanizeBytesPipe.transform(d.checkpointedSize)
            })
          ]
        });
    }

    // Row labels are truncated; show the full operator name as a native tooltip while hovering one.
    const fullByShort = new Map<string, string>();
    for (const row of [...rows, ...unacked, ...instants]) {
      if (!fullByShort.has(row.row)) {
        fullByShort.set(row.row, `${row.operatorFull} #${row.subtask}`);
      }
    }
    this.timelineChart.on('axis-label-item:pointerover', (ev: { target?: { attributes?: { text?: unknown } } }) => {
      container.title = fullByShort.get(String(ev.target?.attributes?.text ?? '')) ?? '';
    });
    this.timelineChart.on('axis-label-item:pointerout', () => (container.title = ''));
    void this.timelineChart.render();
  }

  private clearTimeline(): void {
    this.timelineChart?.destroy();
    this.timelineChart = undefined;
    this.timelineContainer.nativeElement.title = '';
    this.loadingTimeline = false;
    this.timelineEmpty = false;
    this.loadError = undefined;
    this.failedVertexCount = 0;
    this.requestedVertexCount = 0;
  }

  private showEmptyTimeline(error?: LoadError): void {
    this.clearTimeline();
    this.timelineEmpty = true;
    this.loadError = error;
    this.cdr.markForCheck();
  }

  public get emptyTimelineMessage(): string {
    switch (this.loadError) {
      case 'not-retained':
        return 'Could not load subtask data — the JobManager no longer retains this checkpoint';
      case 'request-failed':
        return 'Could not load subtask data — the request to the JobManager failed';
      default:
        return 'No subtask data for this checkpoint';
    }
  }

  // The `history` input is the parent's snapshot; polling only updates currentHistory.
  public get hasHistory(): boolean {
    return this.currentHistory.length > 0;
  }

  public get selectedRotatedOut(): boolean {
    return this.userPinned && !!this.selected && !this.currentHistory.some(h => h.id === this.selected!.id);
  }

  public get partialFailureMessage(): string {
    const operators = this.requestedVertexCount === 1 ? 'operator' : 'operators';
    return `${this.failedVertexCount} of ${this.requestedVertexCount} ${operators} failed to load — the timeline below is incomplete.`;
  }
}

// Backend tags completed subtasks "completed" (incl. aborted) and others "pending_or_failed".
function isCompletedSubtask(
  s: SubTaskCheckpointStatisticsItem
): s is SubTaskCheckpointStatisticsItem & CompletedSubTaskCheckpointStatistics {
  return s.status === 'completed';
}

// Diagonal hatch for unacked rows, used by g2 as a repeating pattern fill.
function createHatchPattern(): HTMLCanvasElement {
  const canvas = document.createElement('canvas');
  canvas.width = 8;
  canvas.height = 8;
  const ctx = canvas.getContext('2d');
  if (ctx) {
    ctx.fillStyle = NO_ACK_FILL;
    ctx.fillRect(0, 0, 8, 8);
    ctx.strokeStyle = NO_ACK_STRIPE;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    // Main diagonal plus the two corner stubs so the stripes stay continuous when tiled.
    ctx.moveTo(0, 8);
    ctx.lineTo(8, 0);
    ctx.moveTo(-2, 2);
    ctx.lineTo(2, -2);
    ctx.moveTo(6, 10);
    ctx.lineTo(10, 6);
    ctx.stroke();
  }
  return canvas;
}

function shortenOperatorName(name: string): string {
  if (!name) {
    return '';
  }
  const compact = name.replace(/\s+/g, ' ').trim();
  return compact.length <= OPERATOR_NAME_MAX ? compact : `${compact.slice(0, OPERATOR_NAME_MAX - 3)}...`;
}

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

import { DatePipe, NgFor, NgIf } from '@angular/common';
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
import { FormsModule } from '@angular/forms';
import { BehaviorSubject, defer, EMPTY, Observable, of, Subject, timer } from 'rxjs';
import { catchError, distinctUntilChanged, filter, map, repeat, switchMap, takeUntil } from 'rxjs/operators';

import { Chart } from '@antv/g2';
import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import {
  Checkpoint,
  CheckpointConfig,
  CheckpointHistory,
  CheckpointSubTask,
  VerticesItem
} from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { formatTimeAxis, startAlignedTicks } from '@flink-runtime-web/utils';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzTooltipModule } from 'ng-zorro-antd/tooltip';

import {
  buildTimelineRows,
  InstantDatum,
  PhaseKey,
  slowestFirstOrder,
  StripDatum,
  TimelineDatum,
  TimelineRows,
  toStripDatum,
  UnackedDatum
} from './checkpoint-timeline-rows';

// 'not-retained': every vertex returned 404, i.e. the JobManager no longer keeps this checkpoint's stats.
type LoadError = 'not-retained' | 'request-failed';

interface TimelineLoad {
  datum: StripDatum;
  perVertex: Record<string, CheckpointSubTask | null>;
  failedCount: number;
  requestedCount: number;
  error?: LoadError;
}

// The legend swatches in the .less file use the theme's palette tokens for the same colors.
const STRIP_COLOR: Record<StripDatum['status'], string> = {
  COMPLETED: '#52c41a',
  SAVEPOINT: '#722ed1',
  IN_PROGRESS: '#faad14',
  FAILED: '#f5222d'
};

const STRIP_STATUSES = Object.keys(STRIP_COLOR) as Array<StripDatum['status']>;

const PHASE_COLOR: Record<PhaseKey, string> = {
  start_delay: '#b8c0cc',
  alignment: '#91d5ff',
  sync: '#fa8c16',
  async: '#52c41a'
};

const PHASES = Object.keys(PHASE_COLOR) as PhaseKey[];

const NOT_COMPLETED_STROKE = '#f5222d';
const NO_ACK_FILL = '#fff1f0';
const NO_ACK_STRIPE = '#ffa39e';
const INSTANT_FILL = '#8c8c8c';

type IntervalMark = ReturnType<Chart['interval']>;

// Built once and updated in place on reloads, so following an in-progress checkpoint doesn't rebuild the canvas.
interface TimelineChart {
  chart: Chart;
  phases: IntervalMark;
  unacked: IntervalMark;
  instants: IntervalMark;
}

@Component({
  selector: 'flink-job-checkpoints-timeline',
  templateUrl: './job-checkpoints-timeline.component.html',
  styleUrls: ['./job-checkpoints-timeline.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NgFor,
    NgIf,
    DatePipe,
    FormsModule,
    HumanizeDurationPipe,
    HumanizeBytesPipe,
    NzAlertModule,
    NzButtonModule,
    NzCheckboxModule,
    NzEmptyModule,
    NzSelectModule,
    NzSpinModule,
    NzTagModule,
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
  @ViewChild('timelineScroll', { static: true }) private readonly timelineScroll: ElementRef<HTMLDivElement>;
  @ViewChild('timelineWrapper', { static: true }) private readonly timelineWrapper: ElementRef<HTMLDivElement>;

  public selected?: StripDatum;
  public loadingTimeline = false;
  public timelineEmpty = false;
  public loadError?: LoadError;
  public failedVertexCount = 0;
  public requestedVertexCount = 0;
  // Rows of the shown checkpoint, how many of the slowest are shown, and the first one scrolled to.
  public totalRows = 0;
  public slowestCount = JobCheckpointsTimelineComponent.SHOWN_ROWS_DEFAULT;
  public firstRow = 0;
  public chartHeight = 0;
  public slowestFirst = false;
  // When the shown checkpoint stats were last fetched, and whether the latest poll for them failed.
  public statsUpdatedAt?: number;
  public statsRefreshFailed = false;

  // Diverges from @Input once polling starts feeding live updates.
  private currentHistory: CheckpointHistory[] = [];
  public userPinned = false;
  private stripChart?: Chart;
  private stripInterval?: IntervalMark;
  // What the strip last drew; polls that change nothing skip the redraw, which would drop an open tooltip.
  private stripDrawn?: string;
  private timeline?: TimelineChart;
  // Full operator name per row label, for the native tooltip on the truncated labels.
  private rowTitles = new Map<string, string>();
  // Kept so scrolling can redraw another window of rows without rebuilding them.
  private timelineRows?: TimelineRows & { datum: StripDatum; slowestOrder: string[] };
  private scrollFrame?: number;
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
  // Holds only the checkpoints that can be selected again: those in the strip, plus the pinned one.
  private readonly detailsCache = new Map<number, Record<string, CheckpointSubTask | null>>();
  private readonly visible$ = new BehaviorSubject<boolean>(false);
  private static readonly POLL_MIN_MS = 1500;
  private static readonly POLL_MAX_MS = 15000;
  private static readonly STRIP_MAX_BARS = 60;
  private static readonly STRIP_HEIGHT = 160;
  private static readonly TIMELINE_ROW_HEIGHT = 22;
  // paddingTop (time axis) + paddingBottom, plus a little slack.
  private static readonly TIMELINE_HEIGHT_BASE = 56;
  // Rows shown by default (the slowest) and the most that can be picked; the chart is at most VISIBLE_ROWS tall.
  private static readonly SHOWN_ROWS_DEFAULT = 25;
  private static readonly SHOWN_ROWS_MAX = 200;
  private static readonly VISIBLE_ROWS = 25;
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
      this.selectInternal(toStripDatum(this.currentHistory[0]), false);
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
        if (this.timeline && this.timelineContainer.nativeElement.clientWidth > 0) {
          void this.timeline.chart.forceFit();
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
      this.statsUpdatedAt = undefined;
      this.statsRefreshFailed = false;
      // Otherwise a load still in flight for the old job lands in the cache under an id the new job reuses.
      this.loadTimeline$.next(null);
      this.detailsCache.clear();
      this.retriedCheckpointId = undefined;
      this.clearTimeline();
      this.renderStrip();
    }
    if (changes['history']) {
      this.markStatsFresh();
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
    if (this.scrollFrame !== undefined) {
      cancelAnimationFrame(this.scrollFrame);
    }
    this.stripChart?.destroy();
    this.timeline?.chart.destroy();
  }

  private renderStrip(): void {
    const data = (this.currentHistory || [])
      .slice(0, JobCheckpointsTimelineComponent.STRIP_MAX_BARS)
      .map(toStripDatum)
      .reverse();

    if (!this.stripChart || !this.stripInterval) {
      this.stripChart = new Chart({
        container: this.stripContainer.nativeElement,
        autoFit: true,
        height: JobCheckpointsTimelineComponent.STRIP_HEIGHT,
        paddingTop: 16,
        paddingRight: 16,
        paddingBottom: 36,
        paddingLeft: 56
      });
      const chart = this.stripChart;
      const isPinned = (d: StripDatum): boolean => this.userPinned && this.selected?.id === d.id;
      this.stripInterval = chart
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
        })
        .interaction('tooltip', BARS_ONLY_TOOLTIP);
      showTooltipOverBarsOnly(chart);
      chart.on('interval:click', (ev: { data?: { data?: StripDatum } }) => {
        const datum = ev.data?.data;
        if (datum) {
          this.select(datum);
        }
      });
    }

    const drawn = JSON.stringify([this.userPinned ? this.selected?.id : null, data]);
    if (drawn === this.stripDrawn) {
      return;
    }
    this.stripDrawn = drawn;
    // The bars move under the pointer; the next move over one shows its tooltip again.
    this.stripChart.emit('tooltip:hide');
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
      this.selectInternal(toStripDatum(this.currentHistory[0]), false);
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
    this.pruneDetailsCache();
    this.renderStrip();
    // The empty state and the rotated-out note read currentHistory.
    this.cdr.markForCheck();

    if (next.length === 0) {
      // The pinned checkpoint is gone too (e.g. after a JobManager failover), so follow whatever comes next.
      this.userPinned = false;
      this.selected = undefined;
      this.clearTimeline();
      return;
    }

    if (!this.userPinned && nextNewestId !== prevNewestId) {
      this.selectInternal(toStripDatum(next[0]), false);
      return;
    }
    // Still showing the same checkpoint (the followed newest one, or the pinned one): reload once it gained acks or
    // finished, or retry a failed load once (retrying on every poll would repeat the error notifications). Not while
    // a load is in flight, so polling doesn't keep cancelling a slow fetch.
    const selected = this.selected;
    const latest = this.userPinned ? next.find(h => h.id === selected?.id) : next[0];
    if (!selected || !latest || latest.id !== selected.id || this.loadingTimeline) {
      return;
    }
    const current = toStripDatum(latest);
    const progressed = current.rawStatus !== selected.rawStatus || current.acked !== selected.acked;
    const retry =
      (this.loadError !== undefined || this.failedVertexCount > 0) && this.retriedCheckpointId !== current.id;
    if (retry) {
      this.retriedCheckpointId = current.id;
    }
    if (progressed || retry) {
      this.selectInternal(current, false);
    }
  }

  private pruneDetailsCache(): void {
    const selectable = new Set(
      this.currentHistory.slice(0, JobCheckpointsTimelineComponent.STRIP_MAX_BARS).map(h => h.id)
    );
    for (const id of this.detailsCache.keys()) {
      if (!selectable.has(id) && id !== this.selected?.id) {
        this.detailsCache.delete(id);
      }
    }
  }

  private startPolling(): void {
    this.visible$
      .pipe(
        distinctUntilChanged(),
        switchMap(visible =>
          visible
            ? defer((): Observable<Checkpoint | 'failed'> => {
                const jobId = this.jobId;
                return jobId
                  ? this.jobService.loadCheckpointStats(jobId).pipe(
                      catchError(() => of('failed' as const)),
                      // A response for the previous job can land after a jobId change has reset the state.
                      filter(() => jobId === this.jobId)
                    )
                  : EMPTY;
              }).pipe(repeat({ delay: () => timer(this.computePollDelayMs()) }))
            : EMPTY
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(stats => {
        if (stats === 'failed') {
          // Keep showing the last stats, marked as stale; the next poll tries again.
          this.statsRefreshFailed = true;
          this.cdr.markForCheck();
          return;
        }
        this.markStatsFresh();
        this.applyHistory(stats.history || []);
      });
  }

  private markStatsFresh(): void {
    this.statsUpdatedAt = Date.now();
    this.statsRefreshFailed = false;
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
    if (datum.rawStatus !== 'IN_PROGRESS') {
      this.detailsCache.set(datum.id, perVertex);
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

  private renderTimeline(datum: StripDatum, perVertex: Record<string, CheckpointSubTask | null>): void {
    const built = buildTimelineRows(datum, this.vertices, perVertex, this.config);
    if (built.rowOrder.length === 0) {
      this.showEmptyTimeline();
      return;
    }
    this.timelineEmpty = false;
    this.timelineRows = { datum, ...built, slowestOrder: slowestFirstOrder(built) };
    this.totalRows = built.rowOrder.length;
    this.rowTitles = new Map();
    for (const row of [...built.rows, ...built.unacked, ...built.instants]) {
      if (!this.rowTitles.has(row.row)) {
        this.rowTitles.set(row.row, `${row.operatorFull} #${row.subtask}`);
      }
    }
    this.drawTimelineWindow();
  }

  // Draws the rows scrolled to, out of the slowest ones picked. The chart is at most 25 rows tall and stays in place
  // while the box around it scrolls, so its canvas stays far below the 65,535 px browsers allow, however big the job.
  private drawTimelineWindow(): void {
    const data = this.timelineRows;
    if (!data) {
      return;
    }
    const { datum, rows, unacked, instants, rowOrder } = data;
    const C = JobCheckpointsTimelineComponent;
    const slowest = data.slowestOrder.slice(0, this.shownRows);
    const picked = new Set(slowest);
    const order = this.slowestFirst ? slowest : rowOrder.filter(row => picked.has(row));
    const visible = this.visibleRows;
    this.firstRow = Math.max(0, Math.min(this.firstRow, order.length - visible));
    const windowRows = order.slice(this.firstRow, this.firstRow + visible);
    const inWindow = new Set(windowRows);
    const shown = <T extends { row: string }>(items: T[]): T[] => items.filter(d => inWindow.has(d.row));
    this.chartHeight = Math.max(visible * C.TIMELINE_ROW_HEIGHT + C.TIMELINE_HEIGHT_BASE, C.TIMELINE_HEIGHT_MIN);

    // The label padding and the time scale cover all rows, so scrolling or picking more rows doesn't shift them.
    const longestLabel = rowOrder.reduce((m, s) => Math.max(m, s.length), 0);
    const leftPadding = Math.min(
      C.LABEL_PADDING_MAX,
      Math.max(C.LABEL_PADDING_MIN, longestLabel * C.LABEL_CHAR_WIDTH + C.LABEL_PADDING_BUFFER)
    );
    const scaleMin = datum.triggered;
    const scaleMax = Math.max(
      [...rows, ...unacked].reduce((m, r) => Math.max(m, r.range[1]), scaleMin),
      scaleMin + 1
    );
    const xScale = { domain: windowRows };
    const yScale = {
      domain: [scaleMin, scaleMax],
      nice: false,
      tickMethod: () => startAlignedTicks(new Date(scaleMin), new Date(scaleMax)).map(Number)
    };

    const timeline = (this.timeline ??= this.createTimelineChart());
    timeline.chart.attr('height', this.chartHeight);
    timeline.chart.attr('paddingLeft', leftPadding);
    timeline.phases.data(shown(rows)).scale('x', xScale).scale('y', yScale);
    timeline.unacked.data(shown(unacked)).scale('x', xScale).scale('y', yScale);
    timeline.instants.data(shown(instants)).scale('x', xScale).scale('y', yScale);
    // The rows move under the pointer; the next move over a bar shows its tooltip again.
    timeline.chart.emit('tooltip:hide');
    void timeline.chart.render();
    this.cdr.markForCheck();
  }

  // How many rows are drawn: the slowest ones picked, or all rows if there are fewer.
  public get shownRows(): number {
    return Math.min(this.slowestCount, this.totalRows);
  }

  public get visibleRows(): number {
    return Math.min(JobCheckpointsTimelineComponent.VISIBLE_ROWS, this.shownRows);
  }

  public get scrollable(): boolean {
    return this.shownRows > this.visibleRows;
  }

  // The scroll box's content: the chart plus one row height for every shown row out of view.
  public get scrollContentHeight(): number {
    return this.chartHeight + (this.shownRows - this.visibleRows) * JobCheckpointsTimelineComponent.TIMELINE_ROW_HEIGHT;
  }

  // 25, 50, ... up to 200 or the number of rows, whichever is smaller; none when every row is already shown.
  public get slowestCountOptions(): number[] {
    const C = JobCheckpointsTimelineComponent;
    const max = Math.min(C.SHOWN_ROWS_MAX, this.totalRows);
    if (max <= C.SHOWN_ROWS_DEFAULT) {
      return [];
    }
    const options: number[] = [];
    for (let n = C.SHOWN_ROWS_DEFAULT; n < max; n += C.SHOWN_ROWS_DEFAULT) {
      options.push(n);
    }
    options.push(max);
    return options;
  }

  public setSlowestCount(count: number): void {
    this.slowestCount = count;
    this.scrollToTop();
  }

  public setSlowestFirst(slowestFirst: boolean): void {
    this.slowestFirst = slowestFirst;
    this.scrollToTop();
  }

  // Other rows or another order: start again from the top.
  private scrollToTop(): void {
    this.firstRow = 0;
    this.timelineScroll.nativeElement.scrollTop = 0;
    this.drawTimelineWindow();
  }

  public onTimelineScroll(): void {
    const first = Math.round(
      this.timelineScroll.nativeElement.scrollTop / JobCheckpointsTimelineComponent.TIMELINE_ROW_HEIGHT
    );
    if (first === this.firstRow) {
      return;
    }
    this.firstRow = first;
    // One redraw per frame, however many scroll events arrive.
    if (this.scrollFrame === undefined) {
      this.scrollFrame = requestAnimationFrame(() => {
        this.scrollFrame = undefined;
        this.drawTimelineWindow();
      });
    }
  }

  private createTimelineChart(): TimelineChart {
    const container = this.timelineContainer.nativeElement;
    const chart = new Chart({ container, autoFit: true, paddingTop: 32, paddingRight: 32, paddingBottom: 16 });
    chart.coordinate({ transform: [{ type: 'transpose' }] });
    const phases = chart
      .interval()
      .encode('x', 'row')
      .encode('y', 'range')
      .encode('color', 'phase')
      .scale('color', { domain: PHASES, range: PHASES.map(p => PHASE_COLOR[p]) })
      .axis('x', {
        title: false,
        labelAutoHide: false,
        labelAutoRotate: false,
        labelFill: '#262626',
        labelOpacity: 1
      })
      // On top, so the time axis is in view as soon as the chart is.
      .axis('y', { title: false, position: 'top', labelFormatter: (v: number) => formatTimeAxis(new Date(Number(v))) })
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
      });
    // On the chart rather than on one mark: the other marks' defaults would override it.
    chart.interaction('tooltip', {
      ...BARS_ONLY_TOOLTIP,
      // Outside the scroll box, which would clip it on the last visible rows. g2 places a tooltip before measuring
      // it, so on its first frame it doesn't flip up yet.
      mount: this.timelineWrapper.nativeElement,
      // g2 5 ellipsizes the tooltip title at 360px; let full operator names wrap instead.
      css: {
        '.g2-tooltip': { 'max-width': '480px' },
        '.g2-tooltip-title': { 'white-space': 'normal', 'overflow-wrap': 'anywhere' }
      }
    });
    showTooltipOverBarsOnly(chart);
    // Returned from a callback: g2 deep-copies constant style objects, which would turn the canvas into a plain object.
    const hatchFill = { image: (this.noAckHatch ??= createHatchPattern()), repetition: 'repeat' };
    const unacked = chart
      .interval()
      .encode('x', 'row')
      .encode('y', 'range')
      .animate(false)
      .style('fill', () => hatchFill)
      .style('stroke', NOT_COMPLETED_STROKE)
      .style('lineWidth', 1.5)
      .style('lineDash', [3, 2])
      .tooltip({
        title: (d: UnackedDatum) => `${d.operatorFull} #${d.subtask}`,
        items: [(d: UnackedDatum) => ({ name: d.label, color: NOT_COMPLETED_STROKE, value: 'no stats reported' })]
      });
    const instants = chart
      .interval()
      .encode('x', 'row')
      .encode('y', 'range')
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

    // Row labels are truncated; show the full operator name as a native tooltip while hovering one.
    chart.on('axis-label-item:pointerover', (ev: { target?: { attributes?: { text?: unknown } } }) => {
      container.title = this.rowTitles.get(String(ev.target?.attributes?.text ?? '')) ?? '';
    });
    chart.on('axis-label-item:pointerout', () => (container.title = ''));
    // Fades the other rows while hovering one, to follow a row across the chart.
    chart.interaction('elementHighlightByX', true);
    return { chart, phases, unacked, instants };
  }

  private clearTimeline(): void {
    this.timeline?.chart.destroy();
    this.timeline = undefined;
    this.timelineRows = undefined;
    this.totalRows = 0;
    this.firstRow = 0;
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

// g2 points a bar chart's tooltip at the bar nearest the pointer along the band axis, so it also shows over the empty
// space next to short bars. With this, a chart shows it only while the pointer is over a bar. No throttling, which
// would drop the show for the bar the pointer ends on.
const BARS_ONLY_TOOLTIP = { disableNative: true, wait: 0 };

interface G2Node {
  className?: string;
  parentNode?: G2Node | null;
  getBBox?: () => { x: number; y: number };
}

interface BarPointerEvent {
  data?: { data?: object };
  canvas?: { x: number; y: number };
  target?: G2Node;
}

function showTooltipOverBarsOnly(chart: Chart): void {
  const hide = (): void => void chart.emit('tooltip:hide');
  // On every move, not just on entering a bar: a redraw drops the tooltip.
  chart.on('interval:pointermove', (ev: BarPointerEvent) => {
    const datum = ev.data?.data;
    if (!datum) {
      return;
    }
    // At the pointer, as g2 would place it; g2 takes the position relative to the plot area.
    let plot = ev.target;
    while (plot && plot.className !== 'plot') {
      plot = plot.parentNode ?? undefined;
    }
    const origin = plot?.getBBox?.();
    const at = origin && ev.canvas ? { offsetX: ev.canvas.x - origin.x, offsetY: ev.canvas.y - origin.y } : {};
    chart.emit('tooltip:show', { ...at, data: { data: datum } });
  });
  chart.on('interval:pointerout', hide);
  // A redraw can also move the bars away from a still pointer, with no pointerout.
  chart.on('plot:pointermove', (ev: BarPointerEvent) => {
    if (ev.target?.className !== 'element') {
      hide();
    }
  });
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

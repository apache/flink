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
import { BehaviorSubject, defer, EMPTY, of, Subject, timer } from 'rxjs';
import { catchError, distinctUntilChanged, map, repeat, switchMap, takeUntil } from 'rxjs/operators';

import * as G2 from '@antv/g2';
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
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

type PhaseKey = 'start_delay' | 'alignment' | 'sync' | 'async';

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
  unaligned: boolean;
  outlier: boolean;
  aborted: boolean;
  stateSize: number;
  checkpointedSize: number;
  totalDuration: number;
}

const STRIP_COLOR: Record<StripDatum['status'], string> = {
  COMPLETED: '#52c41a',
  SAVEPOINT: '#722ed1',
  IN_PROGRESS: '#faad14',
  FAILED: '#f5222d'
};

const PHASE_COLOR: Record<PhaseKey, string> = {
  start_delay: '#b8c0cc',
  alignment: '#91caff',
  sync: '#fa8c16',
  async: '#52c41a'
};

const PHASE_LABEL: Record<PhaseKey, string> = {
  start_delay: 'start_delay',
  alignment: 'alignment',
  sync: 'sync',
  async: 'async'
};

const OPERATOR_NAME_MAX = 60;

// Local HH:MM:SS.mmm — matches the header's DatePipe default (browser timezone).
function formatWallClock(epochMs: number): string {
  const d = new Date(epochMs);
  const pad = (n: number, len = 2): string => String(n).padStart(len, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

function buildTimelineTicks(min: number, max: number, target = 5): number[] {
  const span = Math.max(max - min, 1);
  const niceSteps = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000];
  const desired = span / target;
  const step = niceSteps.find(s => s >= desired) ?? span;
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
    NzToolTipModule
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
  public loadFailed = false;
  public failedVertexCount = 0;
  public requestedVertexCount = 0;

  // Diverges from @Input once polling starts feeding live updates.
  private currentHistory: CheckpointHistory[] = [];
  public userPinned = false;
  private stripChart?: Chart;
  private timelineChart?: Chart;
  private resizeObserver?: ResizeObserver;
  private readonly destroy$ = new Subject<void>();
  private readonly loadTimeline$ = new Subject<StripDatum>();
  // Completed-checkpoint stats are immutable; memoize by id for instant re-selects.
  private readonly detailsCache = new Map<number, Record<string, CheckpointSubTask | null>>();
  private readonly visible$ = new BehaviorSubject<boolean>(false);
  private static readonly POLL_MIN_MS = 1500;
  private static readonly POLL_MAX_MS = 15000;
  private static readonly STRIP_MAX_BARS = 60;
  private static readonly DETAILS_CACHE_MAX = 100;
  // Flink sends Long.MAX_VALUE (well above MAX_SAFE_INTEGER) when periodic checkpointing is disabled.
  private static readonly DISABLED_INTERVAL_THRESHOLD = Number.MAX_SAFE_INTEGER;
  private static readonly TIMELINE_ROW_HEIGHT = 22;
  private static readonly TIMELINE_HEIGHT_BASE = 80;
  private static readonly TIMELINE_HEIGHT_MIN = 160;
  private static readonly LABEL_CHAR_WIDTH = 7;
  private static readonly LABEL_PADDING_BUFFER = 24;
  private static readonly LABEL_PADDING_MIN = 260;
  private static readonly LABEL_PADDING_MAX = 520;

  private readonly humanizeBytesPipe = new HumanizeBytesPipe();
  private readonly humanizeDurationPipe = new HumanizeDurationPipe();

  constructor(private readonly jobService: JobService, private readonly cdr: ChangeDetectorRef) {}

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
          this.stripChart.forceFit();
        }
        if (this.timelineChart && this.timelineContainer.nativeElement.clientWidth > 0) {
          this.timelineChart.forceFit();
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
    const status: StripDatum['status'] = h.is_savepoint
      ? 'SAVEPOINT'
      : h.status === 'FAILED'
      ? 'FAILED'
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

    if (!this.stripChart) {
      this.stripChart = new G2.Chart({
        container: this.stripContainer.nativeElement,
        autoFit: true,
        height: 110,
        padding: [16, 16, 36, 56]
      });
      this.stripChart.animate(false);
      this.stripChart.legend(false);
      this.stripChart
        .interval()
        .position('idLabel*duration')
        .color('status', (s: string) => STRIP_COLOR[s as StripDatum['status']] || '#bfbfbf')
        .size(18)
        .style('idLabel', (idLabel: string) => {
          const isPinned = this.userPinned && this.selected?.idLabel === idLabel;
          return isPinned ? { stroke: '#1890ff', lineWidth: 2.5 } : { stroke: 'transparent', lineWidth: 0 };
        })
        .tooltip('idLabel*duration*acked*rawStatus', (idLabel, duration, acked, rawStatus) => ({
          name: `#${idLabel}`,
          value: `${rawStatus} · ${this.humanizeDurationPipe.transform(duration)} · acked ${acked}`
        }));
      this.stripChart.tooltip({
        showTitle: false,
        showMarkers: false
      });
      this.stripChart.on('element:click', (ev: { data?: { data?: StripDatum } }) => {
        const datum = ev?.data?.data;
        if (datum) {
          this.select(datum);
        }
      });
    }

    this.stripChart.scale({
      idLabel: { type: 'cat', values: data.map(d => d.idLabel) },
      duration: { alias: 'ms', nice: true, min: 0 }
    });
    this.stripChart.axis('idLabel', { title: null, label: { autoHide: true, autoRotate: false } });
    this.stripChart.axis('duration', { title: null });
    this.stripChart.data(data);
    this.stripChart.render();
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

    if (next.length === 0) {
      this.selected = undefined;
      this.clearTimeline();
      this.cdr.markForCheck();
      return;
    }

    // Frozen while pinned; still markForCheck so the rotated-out note updates on poll.
    if (this.userPinned) {
      this.cdr.markForCheck();
      return;
    }

    if (nextNewestId !== prevNewestId) {
      this.selectInternal(this.toStripDatum(next[0]), false);
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
    const interval = this.config?.interval;
    if (
      interval !== undefined &&
      interval > 0 &&
      interval < JobCheckpointsTimelineComponent.DISABLED_INTERVAL_THRESHOLD
    ) {
      return Math.max(
        JobCheckpointsTimelineComponent.POLL_MIN_MS,
        Math.min(interval, JobCheckpointsTimelineComponent.POLL_MAX_MS)
      );
    }
    return JobCheckpointsTimelineComponent.POLL_MAX_MS;
  }

  private loadTimeline(datum: StripDatum): void {
    if (!this.jobId || this.vertices.length === 0) {
      this.showEmptyTimeline(false);
      return;
    }
    this.loadingTimeline = true;
    this.timelineEmpty = false;
    this.loadFailed = false;
    this.failedVertexCount = 0;
    this.cdr.markForCheck();
    this.loadTimeline$.next(datum);
  }

  private cacheDetails(datum: StripDatum, perVertex: Record<string, CheckpointSubTask | null>): void {
    if (datum.status === 'IN_PROGRESS') {
      return; // in-progress stats still change; only cache terminal checkpoints
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
    this.loadTimeline$
      .pipe(
        switchMap(datum => {
          const cached = this.detailsCache.get(datum.id);
          if (cached) {
            return of({
              datum,
              perVertex: cached,
              failed: false,
              failedCount: 0,
              requestedCount: this.vertices.length
            });
          }
          const vertexIds = this.vertices.map(v => v.id);
          const requestedCount = vertexIds.length;
          return this.jobService.loadCheckpointAllSubtaskDetails(this.jobId, datum.id, vertexIds).pipe(
            map(perVertex => {
              const succeeded = Object.values(perVertex).filter(v => v !== null).length;
              const failedCount = requestedCount - succeeded;
              if (failedCount === 0) {
                this.cacheDetails(datum, perVertex);
              }
              return {
                datum,
                perVertex,
                failed: requestedCount > 0 && succeeded === 0,
                failedCount,
                requestedCount
              };
            }),
            catchError(() =>
              of({
                datum,
                perVertex: {} as Record<string, CheckpointSubTask | null>,
                failed: true,
                failedCount: requestedCount,
                requestedCount
              })
            )
          );
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(({ datum, perVertex, failed, failedCount, requestedCount }) => {
        this.loadingTimeline = false;
        if (failed) {
          this.showEmptyTimeline(true);
          return;
        }
        this.loadFailed = false;
        this.failedVertexCount = failedCount;
        this.requestedVertexCount = requestedCount;
        this.renderTimeline(datum, perVertex);
      });
  }

  private buildTimelineRows(
    datum: StripDatum,
    perVertex: Record<string, CheckpointSubTask | null>
  ): { rows: TimelineDatum[]; rowOrder: string[] } {
    const base = datum.triggered;
    const durations: number[] = [];
    const grouped: TimelineDatum[][] = [];

    for (const vertex of this.vertices) {
      const cp = perVertex[vertex.id];
      if (!cp || !cp.subtasks) {
        continue;
      }
      const opNameFull = (vertex.name || '').replace(/\s+/g, ' ').trim();
      const opName = shortenOperatorName(vertex.name);
      const items: TimelineDatum[] = [];

      for (const s of cp.subtasks) {
        if (!isCompletedSubtask(s)) {
          continue;
        }
        const completed = s;
        const startDelay = Math.max(completed.start_delay || 0, 0);
        const alignment = Math.max(completed.alignment.duration || 0, 0);
        const sync = Math.max(completed.checkpoint.sync || 0, 0);
        const asyncMs = Math.max(completed.checkpoint.async || 0, 0);
        const total = startDelay + alignment + sync + asyncMs;
        durations.push(total);

        const row = `${opName} #${completed.index}`;
        const segs: Array<[PhaseKey, number]> = [
          ['start_delay', startDelay],
          ['alignment', alignment],
          ['sync', sync],
          ['async', asyncMs]
        ];
        let cursor = 0;
        for (const [phase, ms] of segs) {
          if (ms <= 0) {
            cursor += ms;
            continue;
          }
          items.push({
            row,
            operatorFull: opNameFull,
            subtask: completed.index,
            phase,
            phaseLabel:
              phase === 'alignment' && completed.unaligned_checkpoint
                ? `${PHASE_LABEL[phase]} (unaligned)`
                : PHASE_LABEL[phase],
            range: [base + cursor, base + cursor + ms],
            durationMs: ms,
            unaligned: !!completed.unaligned_checkpoint,
            outlier: false,
            aborted: !!completed.aborted,
            stateSize: completed.state_size,
            checkpointedSize: completed.checkpointed_size,
            totalDuration: total
          });
          cursor += ms;
        }
      }

      if (items.length > 0) {
        grouped.push(items);
      }
    }

    if (grouped.length === 0) {
      return { rows: [], rowOrder: [] };
    }

    // max(p95, 1.5×median): p95 alone over-marks small samples.
    const median = percentile(durations, 0.5);
    const p95 = percentile(durations, 0.95);
    const outlierThreshold = Math.max(p95, median * 1.5);

    const rows: TimelineDatum[] = [];
    for (const group of grouped) {
      group.sort((a, b) => b.totalDuration - a.totalDuration || a.subtask - b.subtask);
      for (const item of group) {
        if (item.totalDuration >= outlierThreshold || item.aborted) {
          item.outlier = true;
        }
      }
      rows.push(...group);
    }

    const rowOrder: string[] = [];
    const seen = new Set<string>();
    for (const r of rows) {
      if (!seen.has(r.row)) {
        seen.add(r.row);
        rowOrder.push(r.row);
      }
    }

    return { rows, rowOrder };
  }

  private renderTimeline(datum: StripDatum, perVertex: Record<string, CheckpointSubTask | null>): void {
    const { rows, rowOrder } = this.buildTimelineRows(datum, perVertex);
    if (rowOrder.length === 0) {
      this.showEmptyTimeline(false);
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

    this.timelineChart?.destroy();
    this.timelineChart = new G2.Chart({
      container: this.timelineContainer.nativeElement,
      autoFit: true,
      height: chartHeight,
      padding: [24, 32, 48, leftPadding],
      // SVG so axis labels can host <title> tooltips; canvas has no DOM.
      renderer: 'svg'
    });
    this.timelineChart.animate(false);
    this.timelineChart.legend(false);
    this.timelineChart.coordinate('rect').transpose().scale(1, -1);
    const base = datum.triggered;
    const maxTotal = rows.reduce((m, r) => Math.max(m, r.totalDuration), 0);
    const scaleMin = base;
    const scaleMax = base + Math.max(maxTotal, 1);
    this.timelineChart.scale({
      row: { type: 'cat', values: rowOrder },
      range: {
        alias: 'time',
        nice: false,
        min: scaleMin,
        max: scaleMax,
        ticks: buildTimelineTicks(scaleMin, scaleMax)
      },
      phase: { values: ['start_delay', 'alignment', 'sync', 'async'] }
    });
    this.timelineChart.axis('range', {
      title: { text: 'time' },
      label: { formatter: (v: string) => formatWallClock(Number(v)) }
    });
    this.timelineChart.axis('row', {
      title: null,
      label: {
        autoHide: false,
        autoRotate: false,
        style: { fill: '#262626' }
      }
    });
    this.timelineChart
      .interval()
      .position('row*range')
      .color('phase', (p: string) => PHASE_COLOR[p as PhaseKey])
      .style('outlier*unaligned*phase', (outlier: boolean, unaligned: boolean, phase: PhaseKey) => {
        const style: { stroke?: string; lineWidth?: number; lineDash?: number[]; fillOpacity?: number } = {};
        if (outlier) {
          style.stroke = '#f5222d';
          style.lineWidth = 1.5;
          style.lineDash = [3, 2];
        }
        if (phase === 'alignment' && unaligned) {
          style.fillOpacity = 0.55;
        }
        return style;
      })
      .tooltip(
        'phaseLabel*durationMs*totalDuration*stateSize*checkpointedSize*operatorFull*subtask',
        (phaseLabel, ms, total, state, checkpointed, operatorFull, subtask) => ({
          name: `${operatorFull} #${subtask} · ${phaseLabel}`,
          value:
            `${ms}ms (total ${total}ms, state ${this.humanizeBytesPipe.transform(state)},` +
            ` checkpointed ${this.humanizeBytesPipe.transform(checkpointed)})`
        })
      );
    this.timelineChart.tooltip({ shared: false, showMarkers: false });
    this.timelineChart.data(rows);
    this.timelineChart.render();
    // Defer so labels exist in the DOM.
    setTimeout(() => this.decorateRowLabelTooltips(rows), 0);
  }

  private decorateRowLabelTooltips(rows: TimelineDatum[]): void {
    const container = this.timelineContainer?.nativeElement;
    if (!container) {
      return;
    }
    const svg = container.querySelector('svg');
    if (!svg) {
      return;
    }
    const fullByShort = new Map<string, string>();
    for (const row of rows) {
      if (!fullByShort.has(row.row)) {
        fullByShort.set(row.row, `${row.operatorFull} #${row.subtask}`);
      }
    }
    const textNodes = svg.querySelectorAll('text');
    textNodes.forEach(text => {
      const label = (text.textContent || '').trim();
      const full = fullByShort.get(label);
      if (!full) {
        return;
      }
      const existing = text.querySelector(':scope > title');
      if (existing) {
        existing.textContent = full;
        return;
      }
      const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
      title.textContent = full;
      text.appendChild(title);
    });
  }

  private clearTimeline(): void {
    this.timelineChart?.destroy();
    this.timelineChart = undefined;
    this.loadingTimeline = false;
    this.timelineEmpty = false;
    this.loadFailed = false;
  }

  private showEmptyTimeline(failed: boolean): void {
    this.clearTimeline();
    this.timelineEmpty = true;
    this.loadFailed = failed;
    this.failedVertexCount = 0;
    this.requestedVertexCount = 0;
    this.cdr.markForCheck();
  }

  public get selectedRotatedOut(): boolean {
    return this.userPinned && !!this.selected && !this.currentHistory.some(h => h.id === this.selected!.id);
  }

  public get partialFailureMessage(): string {
    const operators = this.failedVertexCount === 1 ? 'operator' : 'operators';
    return `${this.failedVertexCount} of ${this.requestedVertexCount} ${operators} failed to load — the timeline below is incomplete.`;
  }
}

// Backend tags completed subtasks "completed" (incl. aborted) and others "pending_or_failed".
function isCompletedSubtask(
  s: SubTaskCheckpointStatisticsItem
): s is SubTaskCheckpointStatisticsItem & CompletedSubTaskCheckpointStatistics {
  return s.status === 'completed';
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * p)));
  return sorted[idx];
}

function shortenOperatorName(name: string): string {
  if (!name) {
    return '';
  }
  const compact = name.replace(/\s+/g, ' ').trim();
  return compact.length <= OPERATOR_NAME_MAX ? compact : `${compact.slice(0, OPERATOR_NAME_MAX - 3)}...`;
}

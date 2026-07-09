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

import { NgIf } from '@angular/common';
import {
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  OnDestroy,
  ViewChild
} from '@angular/core';
import { Subject } from 'rxjs';
import { distinctUntilChanged, filter, takeUntil } from 'rxjs/operators';

import { Chart } from '@antv/g2';
import { JobDetailCorrect, VerticesItemRange } from '@flink-runtime-web/interfaces';
import { JobService, ColorKey, ConfigService } from '@flink-runtime-web/services';
import { timeFormat } from 'd3-time-format';
import { NzDividerModule } from 'ng-zorro-antd/divider';

import { JobLocalService } from '../job-local.service';

// g2 5's string labelFormatter uses d3's numeric formatter, so a time scale needs
// a function; mirror the previous 'HH:mm:ss.SSS' mask.
const formatTimeAxis = timeFormat('%H:%M:%S.%L');

@Component({
  selector: 'flink-job-timeline',
  templateUrl: './job-timeline.component.html',
  styleUrls: ['./job-timeline.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzDividerModule, NgIf]
})
export class JobTimelineComponent implements AfterViewInit, OnDestroy {
  public listOfVertex: VerticesItemRange[] = [];
  public listOfSubTaskTimeLine: Array<{ name: string; status: string; range: [number, number] }> = [];
  public mainChartInstance: Chart;
  public subTaskChartInstance: Chart;
  private mainInterval!: ReturnType<Chart['interval']>;
  private subTaskInterval!: ReturnType<Chart['interval']>;
  public jobDetail: JobDetailCorrect;
  public selectedName: string;
  public isShowSubTaskTimeLine = false;

  @ViewChild('mainTimeLine', { static: true }) private readonly mainTimeLine: ElementRef;
  @ViewChild('subTaskTimeLine', { static: true }) private readonly subTaskTimeLine: ElementRef;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly configService: ConfigService,
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngAfterViewInit(): void {
    this.setUpMainChart();
    this.setUpSubTaskChart();
    this.jobLocalService
      .jobDetailChanges()
      .pipe(
        filter(() => !!this.mainChartInstance),
        distinctUntilChanged((pre, next) => pre.jid === next.jid),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.jobDetail = data;
        this.listOfVertex = data.vertices
          .filter(v => v['start-time'] > -1)
          .map(vertex => {
            const endTime = vertex['end-time'] > -1 ? vertex['end-time'] : vertex['start-time'] + vertex.duration;
            return {
              ...vertex,
              range: [vertex['start-time'], endTime]
            };
          });
        this.listOfVertex = this.listOfVertex.sort((a, b) => a.range[0] - b.range[0]);
        this.mainInterval.scale('color', this.statusColorScale(this.listOfVertex));
        this.mainInterval.data(this.listOfVertex);
        this.mainChartInstance.changeSize(
          this.mainTimeLine.nativeElement.clientWidth,
          Math.max(this.listOfVertex.length * 50 + 100, 150)
        );
        this.mainChartInstance.render();
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public updateSubTaskChart(vertexId: string): void {
    this.listOfSubTaskTimeLine = [];
    this.jobService
      .loadSubTaskTimes(this.jobDetail.jid, vertexId)
      .pipe(takeUntil(this.destroy$))
      .subscribe(data => {
        data.subtasks.forEach(task => {
          const listOfTimeLine: Array<{ status: string; startTime: number }> = [];
          for (const key in task.timestamps) {
            // @ts-ignore
            const time = task.timestamps[key];
            if (time > 0) {
              listOfTimeLine.push({
                status: key,
                startTime: time
              });
            }
          }
          listOfTimeLine.sort((pre, next) => pre.startTime - next.startTime);
          listOfTimeLine.forEach((item, index) => {
            if (index === listOfTimeLine.length - 1) {
              this.listOfSubTaskTimeLine.push({
                name: `${task.subtask} - ${task.host}`,
                status: item.status,
                range: [item.startTime, task.duration + listOfTimeLine[0].startTime]
              });
            } else {
              this.listOfSubTaskTimeLine.push({
                name: `${task.subtask} - ${task.host}`,
                status: item.status,
                range: [item.startTime, listOfTimeLine[index + 1].startTime]
              });
            }
          });
        });
        this.subTaskInterval.scale('color', this.statusColorScale(this.listOfSubTaskTimeLine));
        this.subTaskInterval.data(this.listOfSubTaskTimeLine);
        this.subTaskChartInstance.changeSize(
          this.subTaskTimeLine.nativeElement.clientWidth,
          Math.max(data.subtasks.length * 50 + 100, 150)
        );
        this.subTaskChartInstance.render();
        this.isShowSubTaskTimeLine = true;
        this.cdr.markForCheck();
        setTimeout(() => {
          try {
            // FIXME scrollIntoViewIfNeeded is a non-standard extension and will not work everywhere
            (
              document.getElementById('subtask') as unknown as {
                scrollIntoViewIfNeeded: () => void;
              }
            ).scrollIntoViewIfNeeded();
          } catch (e) {}
        });
      });
  }

  public setUpMainChart(): void {
    this.mainChartInstance = new Chart({
      container: this.mainTimeLine.nativeElement,
      autoFit: true,
      height: 500,
      paddingTop: 50,
      paddingRight: 50,
      paddingBottom: 50,
      paddingLeft: 50
    });
    this.mainChartInstance.coordinate({ transform: [{ type: 'transpose' }] });
    this.mainInterval = this.mainChartInstance
      .interval()
      .encode('x', 'id')
      .encode('y', 'range')
      .encode('color', 'status')
      .scale('y', { type: 'time', nice: false })
      .animate(false)
      .axis('x', false)
      .axis('y', {
        position: 'top',
        title: false,
        grid: true,
        labelFormatter: (d: Date) => formatTimeAxis(new Date(d))
      })
      .legend('color', { position: 'bottom', layout: { justifyContent: 'center' } })
      .label({
        text: (d: { name: string }) => (d.name.length <= 120 ? d.name : `${d.name.slice(0, 120)}...`),
        position: 'right',
        dx: -20,
        fill: '#ffffff',
        textAlign: 'right',
        fontWeight: 'bold'
      })
      .tooltip({ title: 'name', items: [this.statusRangeTooltipItem] });
    this.mainChartInstance.on('interval:click', (e: { data?: { data?: { name: string; id: string } } }) => {
      const datum = e.data?.data;
      if (datum) {
        this.selectedName = datum.name;
        this.updateSubTaskChart(datum.id);
      }
    });
    this.cdr.markForCheck();
  }

  public setUpSubTaskChart(): void {
    this.subTaskChartInstance = new Chart({
      container: this.subTaskTimeLine.nativeElement,
      autoFit: true,
      height: 10,
      paddingTop: 50,
      paddingRight: 50,
      paddingBottom: 50,
      paddingLeft: 300
    });
    this.subTaskChartInstance.coordinate({ transform: [{ type: 'transpose' }] });
    this.subTaskInterval = this.subTaskChartInstance
      .interval()
      .encode('x', 'name')
      .encode('y', 'range')
      .encode('color', 'status')
      .scale('y', { type: 'time', nice: false })
      .animate(false)
      .axis('x', { title: false })
      .axis('y', {
        position: 'top',
        title: false,
        grid: true,
        labelFormatter: (d: Date) => formatTimeAxis(new Date(d))
      })
      .legend('color', { position: 'bottom', layout: { justifyContent: 'center' } })
      .tooltip({ items: [this.statusRangeTooltipItem] });
    this.cdr.markForCheck();
  }

  // g2 4 derived tooltip rows from the color callback and time mask; rebuild that shape explicitly.
  private readonly statusRangeTooltipItem = (d: {
    status: string;
    range: [number, number];
  }): { name: string; color: string; value: string } => ({
    name: d.status,
    color: this.configService.COLOR_MAP[d.status as ColorKey],
    value: `${formatTimeAxis(new Date(d.range[0]))}-${formatTimeAxis(new Date(d.range[1]))}`
  });

  private statusColorScale(data: Array<{ status: string }>): { domain: string[]; range: string[] } {
    const statuses = [...new Set(data.map(d => d.status))];
    return {
      domain: statuses,
      range: statuses.map(s => this.configService.COLOR_MAP[s as ColorKey])
    };
  }
}

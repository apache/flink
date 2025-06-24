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

import * as G2 from '@antv/g2';
import { Chart } from '@antv/g2';
import { JobDetailCorrect, VerticesItemRange } from '@flink-runtime-web/interfaces';
import { JobService, ColorKey, ConfigService } from '@flink-runtime-web/services';
import { NzDividerModule } from 'ng-zorro-antd/divider';

import { JobLocalService } from '../job-local.service';

@Component({
  selector: 'flink-job-timeline',
  templateUrl: './job-timeline.component.html',
  styleUrls: ['./job-timeline.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzDividerModule, NgIf],
  standalone: true
})
export class JobTimelineComponent implements AfterViewInit, OnDestroy {
  public listOfVertex: VerticesItemRange[] = [];
  public listOfSubTaskTimeLine: Array<{ name: string; status: string; range: [number, number] }> = [];
  public mainChartInstance: Chart;
  public subTaskChartInstance: Chart;
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
        this.mainChartInstance.changeSize(
          this.mainChartInstance.width,
          Math.max(this.listOfVertex.length * 50 + 100, 150)
        );
        this.mainChartInstance.data(this.listOfVertex);
        this.mainChartInstance.scale({
          range: {
            alias: 'Time',
            type: 'time',
            mask: 'HH:mm:ss.SSS',
            nice: false
          }
        });
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
        this.subTaskChartInstance.changeSize(
          this.subTaskChartInstance.width,
          Math.max(data.subtasks.length * 50 + 100, 150)
        );
        this.subTaskChartInstance.data(this.listOfSubTaskTimeLine);
        this.subTaskChartInstance.scale({
          range: {
            alias: 'Time',
            type: 'time',
            mask: 'HH:mm:ss.SSS',
            nice: false
          }
        });
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
    this.mainChartInstance = new G2.Chart({
      container: this.mainTimeLine.nativeElement,
      autoFit: true,
      height: 500,
      padding: [50, 50, 50, 50]
    });
    this.mainChartInstance.animate(false);
    this.mainChartInstance.axis('id', false);
    this.mainChartInstance.coordinate('rect').transpose().scale(1, -1);
    this.mainChartInstance
      .interval()
      .position('id*range')
      .color('status', (type: string) => this.configService.COLOR_MAP[type as ColorKey])
      .label('name', {
        offset: -20,
        position: 'right',
        style: {
          fill: '#ffffff',
          textAlign: 'right',
          fontWeight: 'bold'
        },
        content: data => {
          if (data.name.length <= 120) {
            return data.name;
          } else {
            return `${data.name.slice(0, 120)}...`;
          }
        }
      });
    this.mainChartInstance.tooltip({
      title: 'name'
    });
    this.mainChartInstance.on('click', (e: { x: number; y: number }) => {
      if (this.mainChartInstance.getSnapRecords(e).length) {
        const data = (
          this.mainChartInstance.getSnapRecords(e)[0] as unknown as {
            _origin: { name: string; id: string };
          }
        )._origin;
        this.selectedName = data.name;
        this.updateSubTaskChart(data.id);
      }
    });
    this.cdr.markForCheck();
  }

  public setUpSubTaskChart(): void {
    this.subTaskChartInstance = new G2.Chart({
      container: this.subTaskTimeLine.nativeElement,
      autoFit: true,
      height: 10,
      padding: [50, 50, 50, 300]
    });
    this.subTaskChartInstance.animate(false);
    this.subTaskChartInstance.coordinate('rect').transpose().scale(1, -1);
    this.subTaskChartInstance
      .interval()
      .position('name*range')
      .color('status', (type: string) => this.configService.COLOR_MAP[type as ColorKey]);
    this.cdr.markForCheck();
  }
}

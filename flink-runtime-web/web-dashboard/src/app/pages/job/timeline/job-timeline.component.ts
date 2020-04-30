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
  Component,
  ElementRef,
  OnDestroy,
  ChangeDetectionStrategy,
  ViewChild,
  ChangeDetectorRef,
  AfterViewInit
} from '@angular/core';
import { Chart } from '@antv/g2';
import * as G2 from '@antv/g2';
import { Subject } from 'rxjs';
import { distinctUntilChanged, filter, takeUntil } from 'rxjs/operators';
import { JobDetailCorrectInterface, VerticesItemRangeInterface } from 'interfaces';
import { JobService } from 'services';
import { COLOR_MAP } from 'config';

/// <reference path="../../../../../node_modules/@antv/g2/src/index.d.ts" />

@Component({
  selector       : 'flink-job-timeline',
  templateUrl    : './job-timeline.component.html',
  styleUrls      : [ './job-timeline.component.less' ],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobTimelineComponent implements AfterViewInit, OnDestroy {
  destroy$ = new Subject();
  listOfVertex: VerticesItemRangeInterface[] = [];
  listOfSubTaskTimeLine: Array<{ name: string; status: string; range: [ number, number ] }> = [];
  mainChartInstance: Chart;
  subTaskChartInstance: Chart;
  jobDetail: JobDetailCorrectInterface;
  selectedName: string;
  isShowSubTaskTimeLine = false;
  @ViewChild('mainTimeLine') mainTimeLine: ElementRef;
  @ViewChild('subTaskTimeLine') subTaskTimeLine: ElementRef;

  updateSubTaskChart(vertexId: string) {
    this.listOfSubTaskTimeLine = [];
    this.jobService.loadSubTaskTimes(this.jobDetail.jid, vertexId).subscribe(data => {
      data.subtasks.forEach(task => {
        const listOfTimeLine: Array<{ status: string; startTime: number }> = [];
        for (const key in task.timestamps) {
          // @ts-ignore
          const time = task.timestamps[ key ];
          if (time > 0) {
            listOfTimeLine.push({
              status   : key,
              startTime: time
            });
          }
        }
        listOfTimeLine.sort((pre, next) => pre.startTime - next.startTime);
        listOfTimeLine.forEach((item, index) => {
          if (index === listOfTimeLine.length - 1) {
            this.listOfSubTaskTimeLine.push({
              name  : `${task.subtask} - ${task.host}`,
              status: item.status,
              range : [ item.startTime, task.duration + listOfTimeLine[ 0 ].startTime ]
            });
          } else {
            this.listOfSubTaskTimeLine.push({
              name  : `${task.subtask} - ${task.host}`,
              status: item.status,
              range : [ item.startTime, listOfTimeLine[ index + 1 ].startTime ]
            });
          }
        });
      });
      this.subTaskChartInstance.changeHeight(Math.max(data.subtasks.length * 50 + 100, 150));
      this.subTaskChartInstance.source(this.listOfSubTaskTimeLine, {
        range: {
          alias: 'Time',
          type : 'time',
          mask : 'HH:mm:ss',
          nice : false
        }
      });
      this.subTaskChartInstance.render();
      this.isShowSubTaskTimeLine = true;
      this.cdr.markForCheck();
      setTimeout(() => {
        try {
          (document.getElementById('subtask') as any).scrollIntoViewIfNeeded();
        } catch (e) {
        }
      });
    });
  }

  setUpMainChart() {
    this.mainChartInstance = new G2.Chart({
      container: this.mainTimeLine.nativeElement,
      forceFit : true,
      animate  : false,
      height   : 500,
      padding  : [ 50, 50, 50, 50 ]
    });
    this.mainChartInstance.axis('id', false);
    this.mainChartInstance
    .coord('rect')
    .transpose()
    .scale(1, -1);
    this.mainChartInstance
    .interval()
    .position('id*range')
    // @ts-ignore
    .color('status', (type: any) => COLOR_MAP[ type ])
    .label('name', {
      offset   : -20,
      formatter: (text: string) => {
        if (text.length <= 120) {
          return text;
        } else {
          return text.slice(0, 120) + '...';
        }
      },
      textStyle: {
        fill      : '#ffffff',
        textAlign : 'right',
        fontWeight: 'bold'
      }
    });
    this.mainChartInstance.tooltip({
      title: 'name'
    });
    this.mainChartInstance.on('click', (e: any) => {
      if (this.mainChartInstance.getSnapRecords(e).length) {
        const data = (this.mainChartInstance.getSnapRecords(e)[ 0 ] as any)._origin;
        this.selectedName = data.name;
        this.updateSubTaskChart(data.id);
      }
    });
  }

  setUpSubTaskChart() {
    this.subTaskChartInstance = new G2.Chart({
      container: this.subTaskTimeLine.nativeElement,
      forceFit : true,
      height   : 10,
      animate  : false,
      padding  : [ 50, 50, 50, 300 ]
    });
    this.subTaskChartInstance
    .coord('rect')
    .transpose()
    .scale(1, -1);
    this.subTaskChartInstance
    .interval()
    .position('name*range')
    // @ts-ignore
    .color('status', (type: any) => COLOR_MAP[ type ]);
  }

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {
  }

  ngAfterViewInit() {
    this.setUpMainChart();
    this.setUpSubTaskChart();
    this.jobService.jobDetail$
    .pipe(
      filter(() => !!this.mainChartInstance),
      distinctUntilChanged((pre, next) => pre.jid === next.jid),
      takeUntil(this.destroy$)
    )
    .subscribe(data => {
      this.jobDetail = data;
      this.listOfVertex = data.vertices
      .filter(v => v[ 'start-time' ] > -1)
      .map(vertex => {
        const endTime = vertex[ 'end-time' ] > -1 ? vertex[ 'end-time' ] : (vertex[ 'start-time' ] + vertex.duration);
        return {
          ...vertex,
          range: [ vertex[ 'start-time' ], endTime ]
        };
      });
      this.listOfVertex = this.listOfVertex.sort((a, b) => a.range[ 0 ] - b.range[ 0 ]);
      this.mainChartInstance.changeHeight(Math.max(this.listOfVertex.length * 50 + 100, 150));
      this.mainChartInstance.source(this.listOfVertex, {
        range: {
          alias: 'Time',
          type : 'time',
          mask : 'HH:mm:ss',
          nice : false
        }
      });
      this.mainChartInstance.render();
      this.cdr.markForCheck();
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

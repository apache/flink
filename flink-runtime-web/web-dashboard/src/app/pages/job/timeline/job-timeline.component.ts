/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import {
  AfterViewInit,
  Component,
  ElementRef,
  OnDestroy,
  OnInit,
  ChangeDetectionStrategy,
  ViewChild,
  ChangeDetectorRef
} from '@angular/core';
import * as G2 from '@antv/g2';
import { Subject } from 'rxjs';
import { filter, first, takeUntil } from 'rxjs/operators';
import { VerticesItemInterface } from 'flink-interfaces';
import { ConfigService, JobService } from 'flink-services';

@Component({
  selector       : 'flink-job-timeline',
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl    : './job-timeline.component.html',
  styleUrls      : [ './job-timeline.component.less' ]
})
export class JobTimelineComponent implements OnInit, AfterViewInit, OnDestroy {
  destroy$ = new Subject();
  listOfVertex: VerticesItemInterface[] = [];
  listOfSubTaskTimeLine = [];
  mainChartInstance;
  subTaskChartInstance;
  selectedName;
  isShowSubTaskTimeLine = false;
  @ViewChild('mainTimeLine') mainTimeLine: ElementRef;
  @ViewChild('subTaskTimeLine') subTaskTimeLine: ElementRef;

  updateSubTaskChart(vertexId) {
    this.listOfSubTaskTimeLine = [];
    this.jobService.loadSubTaskTimes(this.jobService.jobDetail.jid, vertexId).subscribe(data => {
      data.subtasks.forEach(task => {
        const listOfTimeLine = [];
        for (const key in task.timestamps) {
          if (task.timestamps[ key ] > 0) {
            listOfTimeLine.push({
              status   : key,
              startTime: task.timestamps[ key ]
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
      this.subTaskChartInstance.changeHeight(Math.max((data.subtasks.length * 30 + 200), 150));
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
          (<any>document.getElementById('subtask')).scrollIntoViewIfNeeded();
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
    this.mainChartInstance.coord().transpose().scale(1, -1);
    this.mainChartInstance.interval().position('id*range').color('status', (type) => this.configService.COLOR_MAP[ type ]).label('name', {
      offset   : -20,
      formatter: (text) => {
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
    this.mainChartInstance.on('click', (e) => {
      const data = this.mainChartInstance.getSnapRecords(e)[ 0 ]._origin;
      this.selectedName = data.name;
      this.updateSubTaskChart(data.id);
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
    this.subTaskChartInstance.coord().transpose().scale(1, -1);
    this.subTaskChartInstance.interval().position('name*range').color('status', (type) => this.configService.COLOR_MAP[ type ]);
  }

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef, private configService: ConfigService) {
  }

  ngOnInit() {
    this.jobService.jobDetail$.pipe(
      filter(() => !!this.mainChartInstance),
      first(),
      takeUntil(this.destroy$)
    ).subscribe(data => {
      this.listOfVertex = data.vertices;
      this.listOfVertex.forEach((vertex) => {
        vertex[ 'range' ] = [ vertex[ 'start-time' ], vertex[ 'end-time' ] ];
      });
      this.listOfVertex = this.listOfVertex.sort((a, b) => a[ 'range' ][ 0 ] - b[ 'range' ][ 0 ]);
      this.mainChartInstance.changeHeight(Math.max((this.listOfVertex.length * 30 + 200), 300));
      this.mainChartInstance.source(this.listOfVertex, {
        range: {
          alias: 'Time',
          type : 'time',
          mask : 'HH:mm:ss',
          nice : false
        }
      });
      this.mainChartInstance.render();
    });
  }

  ngAfterViewInit() {
    this.setUpMainChart();
    this.setUpSubTaskChart();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}

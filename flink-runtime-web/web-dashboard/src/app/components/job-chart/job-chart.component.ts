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
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ElementRef,
  EventEmitter,
  HostBinding,
  Input,
  OnDestroy,
  Output,
  ViewChild
} from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import { Chart } from '@antv/g2';
import * as G2 from '@antv/g2';
import { HumanizeChartNumericPipe } from '@flink-runtime-web/components/humanize-chart-numeric.pipe';
import { JobChartService } from '@flink-runtime-web/components/job-chart/job-chart.service';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';

@Component({
  selector: 'flink-job-chart',
  templateUrl: './job-chart.component.html',
  styleUrls: ['./job-chart.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzButtonModule, NzIconModule, HumanizeChartNumericPipe],
  standalone: true
})
export class JobChartComponent implements AfterViewInit, OnDestroy {
  @Input() title: string;
  @Output() closed = new EventEmitter();
  @ViewChild('chart', { static: true }) chart: ElementRef;
  size = 'small';
  displayMode: 'chart' | 'numeric' = 'chart';
  chartInstance: Chart;
  data: Array<{ time: number; value: number; type: string }> = [];
  latestValue: number;
  destroy$ = new Subject<void>();

  @HostBinding('class.big')
  get isBig(): boolean {
    return this.size === 'big';
  }

  refresh(res: { timestamp: number; values: { [id: string]: number } }): void {
    this.latestValue = res.values[this.title];
    if (this.displayMode === 'numeric') {
      this.cdr.detectChanges();
    }
    this.data.push({
      time: res.timestamp,
      value: this.latestValue,
      type: this.title
    });

    if (this.data.length > 20) {
      this.data.shift();
    }
    if (this.chartInstance) {
      this.chartInstance.changeData(this.data);
    }
  }

  setMode(mode: 'chart' | 'numeric'): void {
    this.displayMode = mode;
    this.cdr.detectChanges();
  }

  resize(size: string): void {
    this.size = size;
    this.cdr.detectChanges();
    setTimeout(() => this.chartInstance.forceFit());
  }

  close(): void {
    this.closed.emit(this.title);
  }

  constructor(private cdr: ChangeDetectorRef, private jobChartService: JobChartService) {}

  ngAfterViewInit(): void {
    this.cdr.detach();
    this.chartInstance = new G2.Chart({
      container: this.chart.nativeElement,
      height: 150,
      autoFit: true,
      padding: 'auto'
    });
    this.chartInstance.legend(false);
    this.chartInstance.data(this.data);
    this.chartInstance.scale({
      time: {
        alias: 'Time',
        type: 'time',
        mask: 'HH:mm:ss',
        tickCount: 3
      },
      type: {
        type: 'cat'
      }
    });
    this.chartInstance
      .line()
      .position('time*value')
      .shape('smooth')
      .color('type')
      .size(2)
      .animate({
        update: {
          duration: 0
        }
      });
    this.chartInstance.render();
    this.jobChartService.resize$.pipe(takeUntil(this.destroy$)).subscribe(() => {
      if (this.chartInstance) {
        setTimeout(() => {
          this.chartInstance.forceFit();
        });
      }
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    if (this.chartInstance) {
      this.chartInstance.destroy();
    }
  }
}

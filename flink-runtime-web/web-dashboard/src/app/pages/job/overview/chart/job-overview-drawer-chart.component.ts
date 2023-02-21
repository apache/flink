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

import { NgForOf, NgIf } from '@angular/common';
import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  OnDestroy,
  OnInit,
  QueryList,
  ViewChildren
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Subject } from 'rxjs';
import { distinctUntilChanged, filter, mergeMap, takeUntil } from 'rxjs/operators';

import { JobChartComponent } from '@flink-runtime-web/components/job-chart/job-chart.component';
import { MetricsService } from '@flink-runtime-web/services';
import { NzSelectModule } from 'ng-zorro-antd/select';

import { JobLocalService } from '../../job-local.service';

@Component({
  selector: 'flink-job-overview-drawer-chart',
  templateUrl: './job-overview-drawer-chart.component.html',
  styleUrls: ['./job-overview-drawer-chart.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf, NzSelectModule, FormsModule, NgForOf, JobChartComponent],
  standalone: true
})
export class JobOverviewDrawerChartComponent implements OnInit, OnDestroy {
  public data = [];
  public listOfMetricName: string[] = [];
  public listOfSelectedMetric: string[] = [];
  public listOfUnselectedMetric: string[] = [];
  public cacheMetricKey: string;

  @ViewChildren(JobChartComponent) private readonly listOfJobChartComponent: QueryList<JobChartComponent>;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly metricsService: MetricsService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.jobLocalService
      .jobWithVertexChanges()
      .pipe(
        distinctUntilChanged((x, y) => x.vertex!.id === y.vertex!.id),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.loadMetricList(data.job.jid, data.vertex!.id);
      });
    this.jobLocalService
      .jobWithVertexChanges()
      .pipe(
        filter(() => this.listOfSelectedMetric.length > 0),
        mergeMap(data => this.metricsService.loadMetrics(data.job.jid, data.vertex!.id, this.listOfSelectedMetric)),
        takeUntil(this.destroy$)
      )
      .subscribe(res => {
        if (this.listOfJobChartComponent && this.listOfJobChartComponent.length) {
          this.listOfJobChartComponent.forEach(chart => {
            chart.refresh(res);
          });
        }
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public loadMetricList(jobId: string, vertexId: string): void {
    this.cacheMetricKey = `${jobId}/${vertexId}`;
    this.metricsService.loadAllAvailableMetrics(jobId, vertexId).subscribe(data => {
      this.listOfMetricName = data.map(item => item.id);
      this.listOfSelectedMetric = this.jobLocalService.metricsCacheMap.get(this.cacheMetricKey) || [];
      this.updateUnselectedMetricList();
      this.cdr.markForCheck();
    });
  }

  public updateMetric(metric: string): void {
    this.listOfSelectedMetric = [...this.listOfSelectedMetric, metric];
    this.jobLocalService.metricsCacheMap.set(this.cacheMetricKey, this.listOfSelectedMetric);
    this.updateUnselectedMetricList();
  }

  public closeMetric(metric: string): void {
    this.listOfSelectedMetric = this.listOfSelectedMetric.filter(item => item !== metric);
    this.jobLocalService.metricsCacheMap.set(this.cacheMetricKey, this.listOfSelectedMetric);
    this.updateUnselectedMetricList();
  }

  public updateUnselectedMetricList(): void {
    this.listOfUnselectedMetric = this.listOfMetricName.filter(item => this.listOfSelectedMetric.indexOf(item) === -1);
  }
}

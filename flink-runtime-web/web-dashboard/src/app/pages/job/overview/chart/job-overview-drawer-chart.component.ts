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
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  OnDestroy,
  OnInit,
  QueryList,
  ViewChildren
} from '@angular/core';
import { Subject } from 'rxjs';
import { distinctUntilChanged, filter, mergeMap, takeUntil } from 'rxjs/operators';

import { JobChartComponent } from 'share/customize/job-chart/job-chart.component';

import { JobService, MetricsService } from 'services';

@Component({
  selector: 'flink-job-overview-drawer-chart',
  templateUrl: './job-overview-drawer-chart.component.html',
  styleUrls: ['./job-overview-drawer-chart.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
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
    private readonly jobService: JobService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.jobService.jobWithVertex$
      .pipe(
        takeUntil(this.destroy$),
        distinctUntilChanged((x, y) => x.vertex!.id === y.vertex!.id)
      )
      .subscribe(data => {
        this.loadMetricList(data.job.jid, data.vertex!.id);
      });
    this.jobService.jobWithVertex$
      .pipe(
        takeUntil(this.destroy$),
        filter(() => this.listOfSelectedMetric.length > 0),
        mergeMap(data => this.metricsService.getMetrics(data.job.jid, data.vertex!.id, this.listOfSelectedMetric))
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
    this.metricsService.getAllAvailableMetrics(jobId, vertexId).subscribe(data => {
      this.listOfMetricName = data.map(item => item.id);
      this.listOfSelectedMetric = this.jobService.metricsCacheMap.get(this.cacheMetricKey) || [];
      this.updateUnselectedMetricList();
      this.cdr.markForCheck();
    });
  }

  public updateMetric(metric: string): void {
    this.listOfSelectedMetric = [...this.listOfSelectedMetric, metric];
    this.jobService.metricsCacheMap.set(this.cacheMetricKey, this.listOfSelectedMetric);
    this.updateUnselectedMetricList();
  }

  public closeMetric(metric: string): void {
    this.listOfSelectedMetric = this.listOfSelectedMetric.filter(item => item !== metric);
    this.jobService.metricsCacheMap.set(this.cacheMetricKey, this.listOfSelectedMetric);
    this.updateUnselectedMetricList();
  }

  public updateUnselectedMetricList(): void {
    this.listOfUnselectedMetric = this.listOfMetricName.filter(item => this.listOfSelectedMetric.indexOf(item) === -1);
  }
}

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
  OnDestroy,
  OnInit,
  ChangeDetectionStrategy,
  ViewChildren,
  QueryList,
  ChangeDetectorRef
} from '@angular/core';
import { Subject } from 'rxjs';
import { distinctUntilChanged, filter, flatMap, takeUntil } from 'rxjs/operators';
import { JobService, MetricsService } from 'services';
import { JobChartComponent } from 'share/customize/job-chart/job-chart.component';

@Component({
  selector: 'flink-job-overview-drawer-chart',
  templateUrl: './job-overview-drawer-chart.component.html',
  styleUrls: ['./job-overview-drawer-chart.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobOverviewDrawerChartComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  data = [];
  listOfMetricName: string[] = [];
  listOfSelectedMetric: string[] = [];
  listOfUnselectedMetric: string[] = [];
  cacheMetricKey: string;
  @ViewChildren(JobChartComponent) listOfJobChartComponent: QueryList<JobChartComponent>;

  loadMetricList(jobId: string, vertexId: string) {
    this.cacheMetricKey = `${jobId}/${vertexId}`;
    this.metricsService.getAllAvailableMetrics(jobId, vertexId).subscribe(data => {
      this.listOfMetricName = data.map(item => item.id);
      this.listOfSelectedMetric = this.jobService.metricsCacheMap.get(this.cacheMetricKey) || [];
      this.updateUnselectedMetricList();
      this.cdr.markForCheck();
    });
  }

  updateMetric(metric: string) {
    this.listOfSelectedMetric = [...this.listOfSelectedMetric, metric];
    this.jobService.metricsCacheMap.set(this.cacheMetricKey, this.listOfSelectedMetric);
    this.updateUnselectedMetricList();
  }

  closeMetric(metric: string) {
    this.listOfSelectedMetric = this.listOfSelectedMetric.filter(item => item !== metric);
    this.jobService.metricsCacheMap.set(this.cacheMetricKey, this.listOfSelectedMetric);
    this.updateUnselectedMetricList();
  }

  updateUnselectedMetricList() {
    this.listOfUnselectedMetric = this.listOfMetricName.filter(item => this.listOfSelectedMetric.indexOf(item) === -1);
  }

  constructor(private metricsService: MetricsService, private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
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
        flatMap(data => this.metricsService.getMetrics(data.job.jid, data.vertex!.id, this.listOfSelectedMetric))
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

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

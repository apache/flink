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

import { Component, OnDestroy, OnInit, ChangeDetectionStrategy, ViewChildren, QueryList, ChangeDetectorRef } from '@angular/core';
import { Subject } from 'rxjs';
import { distinctUntilKeyChanged, filter, flatMap, takeUntil } from 'rxjs/operators';
import { NodesItemCorrectInterface } from 'flink-interfaces';
import { JobService, MetricsService } from 'flink-services';
import { JobChartComponent } from 'flink-share/customize/job-chart/job-chart.component';

@Component({
  selector       : 'flink-job-overview-drawer-chart',
  templateUrl    : './job-overview-drawer-chart.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls      : [ './job-overview-drawer-chart.component.less' ]
})
export class JobOverviewDrawerChartComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  data = [];
  listOfMetricName = [];
  listOfSelectedMetric = [];
  listOfUnselectedMetric = [];
  _node: NodesItemCorrectInterface;
  @ViewChildren(JobChartComponent) listOfJobChartComponent: QueryList<JobChartComponent>;

  loadMetricList(node) {
    this.metricsService.getAllAvailableMetrics(this.jobService.jobDetail.jid, node.id).subscribe(data => {
      this.listOfMetricName = data.map(item => item.id);
      this.listOfSelectedMetric = [];
      this.updateUnselectedMetricList();
      this.cdr.markForCheck();
    });
  }

  updateMetric(metric) {
    this.listOfSelectedMetric = [ ...this.listOfSelectedMetric, metric ];
    this.updateUnselectedMetricList();
  }

  closeMetric(metric) {
    this.listOfSelectedMetric = this.listOfSelectedMetric.filter(item => item !== metric);
    this.updateUnselectedMetricList();
  }

  updateUnselectedMetricList() {
    this.listOfUnselectedMetric = this.listOfMetricName.filter(item => this.listOfSelectedMetric.indexOf(item) === -1);
  }

  constructor(
    private metricsService: MetricsService,
    private jobService: JobService,
    private cdr: ChangeDetectorRef) {
  }

  ngOnInit() {
    this.jobService.selectedVertexNode$.pipe(distinctUntilKeyChanged('id'), takeUntil(this.destroy$)).subscribe((node) => {
      this.loadMetricList(node);
    });
    this.jobService.selectedVertexNode$.pipe(
      takeUntil(this.destroy$),
      filter(() => this.listOfSelectedMetric.length > 0),
      flatMap((node) =>
        this.metricsService.getMetrics(this.jobService.jobDetail.jid, node.id, this.listOfSelectedMetric)
      )
    ).subscribe((res) => {
      this.cdr.markForCheck();
      if (this.listOfJobChartComponent && this.listOfJobChartComponent.length) {
        this.listOfJobChartComponent.forEach(chart => {
          chart.refresh(res);
        });
      }
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}

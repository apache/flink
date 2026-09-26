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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { Subject } from 'rxjs';
import { switchMap, takeUntil, withLatestFrom } from 'rxjs/operators';

import { TopNMetricsComponent } from '@flink-runtime-web/components/topn-metrics/topn-metrics.component';
import {
  BackpressureOperatorInfo,
  BusyOperatorInfo,
  CpuConsumerInfo,
  GcTaskInfo,
  SourceLagInfo
} from '@flink-runtime-web/interfaces';
import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { StatusService, TopNMetricsService } from '@flink-runtime-web/services';

@Component({
  selector: 'flink-job-topn-metric',
  templateUrl: './job-topn-metric.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: true,
  imports: [TopNMetricsComponent]
})
export class JobTopnMetricComponent implements OnInit, OnDestroy {
  public topBackpressureOperators: BackpressureOperatorInfo[] = [];
  public topBusyOperators: BusyOperatorInfo[] = [];
  public topLaggingSources: SourceLagInfo[] = [];
  public topCpuConsumers: CpuConsumerInfo[] = [];
  public topGcIntensiveTasks: GcTaskInfo[] = [];

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobLocalService: JobLocalService,
    private readonly topNMetricsService: TopNMetricsService,
    private readonly statusService: StatusService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    // Re-fetch on every refresh tick, pinned to the currently focused job.
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        withLatestFrom(this.jobLocalService.jobDetailChanges()),
        switchMap(([, job]) => this.topNMetricsService.loadTopNMetrics(job.jid))
      )
      .subscribe(metrics => {
        this.topBackpressureOperators = metrics.topBackpressureOperators ?? [];
        this.topBusyOperators = metrics.topBusyOperators ?? [];
        this.topLaggingSources = metrics.topLaggingSources ?? [];
        this.topCpuConsumers = metrics.topCpuConsumers ?? [];
        this.topGcIntensiveTasks = metrics.topGcIntensiveTasks ?? [];
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

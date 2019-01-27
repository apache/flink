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

import { Component, OnDestroy, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { ConfigService, JobService, MetricsService } from 'flink-services';

@Component({
  selector       : 'flink-job-overview-drawer-watermarks',
  templateUrl    : './job-overview-drawer-watermarks.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls      : [ './job-overview-drawer-watermarks.component.less' ]
})
export class JobOverviewDrawerWatermarksComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  listOfWaterMark = [];
  isLoading = true;

  trackWatermarkBy(index, node) {
    return node.subTaskIndex;
  }

  transformWatermark(value: number) {
    if (isNaN(value) || value <= this.configService.LONG_MIN_VALUE) {
      return 'No Watermark';
    } else {
      return value;
    }
  }

  constructor(
    private configService: ConfigService,
    private jobService: JobService,
    private metricsService: MetricsService,
    private cdr: ChangeDetectorRef) {
  }

  ngOnInit() {
    this.jobService.selectedVertexNode$.pipe(
      takeUntil(this.destroy$),
      flatMap((node) =>
        this.metricsService.getWatermarks(
          this.jobService.jobDetail.jid, node.id, node.parallelism
        )
      )
    ).subscribe(data => {
      const list = [];
      this.isLoading = false;
      for (const key in data.watermarks) {
        list.push({
          subTaskIndex: key,
          watermark   : this.transformWatermark(data.watermarks[ key ])
        });
      }
      this.listOfWaterMark = list;
      this.cdr.markForCheck();
    }, () => {
      this.isLoading = false;
      this.cdr.markForCheck();
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}

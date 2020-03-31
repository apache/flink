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

import { Component, OnDestroy, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { JobService, MetricsService } from 'services';

@Component({
  selector: 'flink-job-overview-drawer-watermarks',
  templateUrl: './job-overview-drawer-watermarks.component.html',
  styleUrls: ['./job-overview-drawer-watermarks.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobOverviewDrawerWatermarksComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  listOfWaterMark: Array<{ subTaskIndex: number; watermark: number }> = [];
  isLoading = true;

  trackWatermarkBy(_: number, node: { subTaskIndex: string; watermark: number }) {
    return node.subTaskIndex;
  }

  constructor(private jobService: JobService, private metricsService: MetricsService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.jobService.jobWithVertex$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(data => this.metricsService.getWatermarks(data.job.jid, data.vertex!.id, data.vertex!.parallelism))
      )
      .subscribe(
        data => {
          const list = [];
          this.isLoading = false;
          for (const key in data.watermarks) {
            list.push({
              subTaskIndex: +key + 1,
              watermark: data.watermarks[key]
            });
          }
          this.listOfWaterMark = list;
          this.cdr.markForCheck();
        },
        () => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }
      );
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

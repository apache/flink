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
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { of, Subject } from 'rxjs';
import { catchError, map, mergeMap, takeUntil } from 'rxjs/operators';

import {
  HumanizeWatermarkPipe,
  HumanizeWatermarkToDatetimePipe
} from '@flink-runtime-web/components/humanize-watermark.pipe';
import { MetricsService } from '@flink-runtime-web/services';
import { typeDefinition } from '@flink-runtime-web/utils';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTooltipModule } from 'ng-zorro-antd/tooltip';

import { JobLocalService } from '../../job-local.service';

interface WatermarkData {
  subTaskIndex: number;
  watermark: number;
}

@Component({
  selector: 'flink-job-overview-drawer-watermarks',
  templateUrl: './job-overview-drawer-watermarks.component.html',
  styleUrls: ['./job-overview-drawer-watermarks.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: true,
  imports: [
    NgIf,
    NgForOf,
    FormsModule,
    NzSelectModule,
    NzTableModule,
    NzIconModule,
    NzTooltipModule,
    HumanizeWatermarkPipe,
    HumanizeWatermarkToDatetimePipe
  ]
})
export class JobOverviewDrawerWatermarksComponent implements OnInit, OnDestroy {
  public readonly trackBySubtaskIndex = (_: number, node: { subTaskIndex: number; watermark: number }): number =>
    node.subTaskIndex;

  public listOfWaterMark: WatermarkData[] = [];
  public isLoading = true;
  public virtualItemSize = 36;
  public readonly narrowLogData = typeDefinition<WatermarkData>();

  // Timezone related properties
  // Using IANA timezone names to properly handle Daylight Saving Time (DST)
  // Reference: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
  public timezoneOptions = [
    { label: 'UTC', value: 'UTC' },
    { label: 'Pacific Time - Los Angeles (PST/PDT)', value: 'America/Los_Angeles' },
    { label: 'Mountain Time - Denver (MST/MDT)', value: 'America/Denver' },
    { label: 'Central Time - Chicago (CST/CDT)', value: 'America/Chicago' },
    { label: 'Eastern Time - New York (EST/EDT)', value: 'America/New_York' },
    { label: 'London (GMT/BST)', value: 'Europe/London' },
    { label: 'Berlin/Paris/Rome (CET/CEST)', value: 'Europe/Berlin' },
    { label: 'Athens (EET/EEST)', value: 'Europe/Athens' },
    { label: 'Moscow (MSK)', value: 'Europe/Moscow' },
    { label: 'Dubai (GST)', value: 'Asia/Dubai' },
    { label: 'India - Mumbai/Delhi (IST)', value: 'Asia/Kolkata' },
    { label: 'China - Beijing/Shanghai (CST)', value: 'Asia/Shanghai' },
    { label: 'Singapore (SGT)', value: 'Asia/Singapore' },
    { label: 'Japan - Tokyo (JST)', value: 'Asia/Tokyo' },
    { label: 'Korea - Seoul (KST)', value: 'Asia/Seoul' },
    { label: 'Australia - Sydney (AEDT/AEST)', value: 'Australia/Sydney' },
    { label: 'New Zealand - Auckland (NZDT/NZST)', value: 'Pacific/Auckland' }
  ];

  public selectedTimezone: string = '';

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobLocalService: JobLocalService,
    private readonly metricsService: MetricsService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  private getBrowserTimezone(): string {
    // Get browser's IANA timezone identifier
    // This will properly handle DST changes
    try {
      const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
      
      // Check if the browser timezone exists in the options list
      const exists = this.timezoneOptions.some(option => option.value === browserTimezone);
      
      // If browser timezone exists in the list, use it; otherwise default to UTC
      return exists ? browserTimezone : 'UTC';
    } catch (error) {
      console.error('[getBrowserTimezone] Error getting browser timezone:', error);
      return 'UTC';
    }
  }

  public ngOnInit(): void {
    // Set default timezone to browser's timezone
    this.selectedTimezone = this.getBrowserTimezone();

    this.jobLocalService
      .jobWithVertexChanges()
      .pipe(
        mergeMap(data =>
          this.metricsService.loadWatermarks(data.job.jid, data.vertex!.id).pipe(
            map(data => {
              const list = [];
              for (const key in data.watermarks) {
                list.push({
                  subTaskIndex: +key,
                  watermark: data.watermarks[key]
                } as WatermarkData);
              }
              return list;
            }),
            catchError(() => of([] as WatermarkData[]))
          )
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(list => {
        this.isLoading = false;
        this.listOfWaterMark = list;
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  sortWatermark(a: WatermarkData, b: WatermarkData): number {
    return a.watermark - b.watermark;
  }
}

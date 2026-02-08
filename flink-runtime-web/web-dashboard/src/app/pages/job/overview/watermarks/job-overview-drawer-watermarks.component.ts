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
  public timezoneOptions = [
    { label: 'UTC-12 (International Date Line West)', value: 'UTC-12' },
    { label: 'UTC-11 (Coordinated Universal Time-11)', value: 'UTC-11' },
    { label: 'UTC-10 (Hawaii)', value: 'UTC-10' },
    { label: 'UTC-9 (Alaska)', value: 'UTC-9' },
    { label: 'UTC-8 (Pacific Time - Los Angeles)', value: 'UTC-8' },
    { label: 'UTC-7 (Mountain Time - Denver)', value: 'UTC-7' },
    { label: 'UTC-6 (Central Time - Chicago)', value: 'UTC-6' },
    { label: 'UTC-5 (Eastern Time - New York)', value: 'UTC-5' },
    { label: 'UTC-4 (Atlantic Time)', value: 'UTC-4' },
    { label: 'UTC-3 (Buenos Aires, Brasilia)', value: 'UTC-3' },
    { label: 'UTC-2 (Mid-Atlantic)', value: 'UTC-2' },
    { label: 'UTC-1 (Azores)', value: 'UTC-1' },
    { label: 'UTC+0 (London, Dublin)', value: 'UTC+0' },
    { label: 'UTC+1 (Berlin, Paris, Rome)', value: 'UTC+1' },
    { label: 'UTC+2 (Cairo, Athens, Istanbul)', value: 'UTC+2' },
    { label: 'UTC+3 (Moscow, Baghdad)', value: 'UTC+3' },
    { label: 'UTC+4 (Dubai, Baku)', value: 'UTC+4' },
    { label: 'UTC+5 (Islamabad, Karachi)', value: 'UTC+5' },
    { label: 'UTC+5:30 (Mumbai, New Delhi)', value: 'UTC+5:30' },
    { label: 'UTC+6 (Dhaka, Almaty)', value: 'UTC+6' },
    { label: 'UTC+7 (Bangkok, Jakarta)', value: 'UTC+7' },
    { label: 'UTC+8 (Beijing, Singapore)', value: 'UTC+8' },
    { label: 'UTC+9 (Tokyo, Seoul)', value: 'UTC+9' },
    { label: 'UTC+10 (Sydney, Melbourne)', value: 'UTC+10' },
    { label: 'UTC+11 (Solomon Islands)', value: 'UTC+11' },
    { label: 'UTC+12 (Fiji, Auckland)', value: 'UTC+12' },
    { label: "UTC+13 (Nuku'alofa)", value: 'UTC+13' },
    { label: 'UTC+14 (Kiritimati)', value: 'UTC+14' }
  ];

  public selectedTimezone: string = '';

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobLocalService: JobLocalService,
    private readonly metricsService: MetricsService,
    private readonly cdr: ChangeDetectorRef
  ) {
    console.log('[Watermarks Component] Constructor called');
  }

  private getBrowserTimezone(): string {
    const offset = new Date().getTimezoneOffset();
    console.log('[getBrowserTimezone] Browser offset (minutes):', offset);

    const hours = Math.abs(Math.floor(offset / 60));
    const minutes = Math.abs(offset % 60);
    const sign = offset <= 0 ? '+' : '-';

    let browserTz: string;
    if (minutes === 0) {
      browserTz = `UTC${sign}${hours}`;
    } else {
      browserTz = `UTC${sign}${hours}:${minutes.toString().padStart(2, '0')}`;
    }

    console.log('[getBrowserTimezone] Generated timezone:', browserTz);

    // Check if the browser timezone exists in the options list
    const exists = this.timezoneOptions.some(option => option.value === browserTz);
    console.log('[getBrowserTimezone] Exists in options:', exists);

    // If browser timezone exists in the list, use it; otherwise default to UTC+8
    const result = exists ? browserTz : 'UTC+8';
    console.log('[getBrowserTimezone] Final result:', result);
    return result;
  }

  public ngOnInit(): void {
    console.log('[Watermarks Component] ngOnInit called');

    // Set default timezone to browser's timezone
    this.selectedTimezone = this.getBrowserTimezone();
    console.log('[Watermarks Component] Selected timezone:', this.selectedTimezone);

    this.jobLocalService
      .jobWithVertexChanges()
      .pipe(
        mergeMap(data => {
          console.log('[Watermarks Component] Job vertex changed:', data.job.jid, data.vertex?.id);
          return this.metricsService.loadWatermarks(data.job.jid, data.vertex!.id).pipe(
            map(data => {
              console.log('[Watermarks Component] Watermarks loaded:', data.watermarks);
              const list = [];
              for (const key in data.watermarks) {
                list.push({
                  subTaskIndex: +key,
                  watermark: data.watermarks[key]
                } as WatermarkData);
              }
              console.log('[Watermarks Component] Processed list:', list);
              return list;
            }),
            catchError(error => {
              console.error('[Watermarks Component] Error loading watermarks:', error);
              return of([] as WatermarkData[]);
            })
          );
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(list => {
        console.log('[Watermarks Component] Subscribe callback, list:', list);
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

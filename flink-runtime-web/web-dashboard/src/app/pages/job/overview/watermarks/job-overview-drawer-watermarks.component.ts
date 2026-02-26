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
  // Using browser's native Intl API to get all supported timezones
  // This provides a complete timezone list and properly handles Daylight Saving Time (DST)
  // Reference: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/supportedValuesOf
  public timezoneOptions: Array<{ label: string; value: string }> = [];

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
    // Reference: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/DateTimeFormat/resolvedOptions
    try {
      return Intl.DateTimeFormat().resolvedOptions().timeZone;
    } catch (error) {
      console.error('[getBrowserTimezone] Error getting browser timezone, falling back to UTC:', error);
      // As a last resort, rely on runtime environment offset
      const offset = -new Date().getTimezoneOffset();
      const sign = offset >= 0 ? '+' : '-';
      const hours = String(Math.floor(Math.abs(offset) / 60)).padStart(2, '0');
      return `Etc/GMT${sign}${hours}`;
    }
  }

  private initializeTimezoneOptions(): void {
    // Use modern browser API to get all supported timezones
    // Reference: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/supportedValuesOf
    try {
      if (typeof Intl.supportedValuesOf === 'function') {
        const timezones = Intl.supportedValuesOf('timeZone');
        this.timezoneOptions = timezones.map(tz => ({ label: tz, value: tz })).sort((a, b) => a.label.localeCompare(b.label));
      } else {
        // Fallback for browsers that don't support Intl.supportedValuesOf
        console.warn('[initializeTimezoneOptions] Intl.supportedValuesOf not supported, falling back to browser local timezone');
        const browserTz = this.getBrowserTimezone();
        this.timezoneOptions = [{ label: browserTz, value: browserTz }];
      }
    } catch (error) {
      console.error('[initializeTimezoneOptions] Error initializing timezone options:', error);
      // Fallback to browser local timezone on error
      const browserTz = this.getBrowserTimezone();
      this.timezoneOptions = [{ label: browserTz, value: browserTz }];
    }
  }

  public ngOnInit(): void {
    // Initialize timezone options
    this.initializeTimezoneOptions();

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

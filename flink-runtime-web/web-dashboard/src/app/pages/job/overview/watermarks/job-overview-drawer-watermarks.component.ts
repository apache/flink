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

import { NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { of, Subject } from 'rxjs';
import { catchError, map, mergeMap, takeUntil } from 'rxjs/operators';

import {
  HumanizeWatermarkPipe,
  HumanizeWatermarkToDatetimePipe
} from '@flink-runtime-web/components/humanize-watermark.pipe';
import { MetricsService } from '@flink-runtime-web/services';
import { typeDefinition } from '@flink-runtime-web/utils/strong-type';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

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
  imports: [NzTableModule, NgIf, HumanizeWatermarkPipe, HumanizeWatermarkToDatetimePipe, NzIconModule, NzToolTipModule],
  standalone: true
})
export class JobOverviewDrawerWatermarksComponent implements OnInit, OnDestroy {
  public readonly trackBySubtaskIndex = (_: number, node: { subTaskIndex: number; watermark: number }): number =>
    node.subTaskIndex;

  public listOfWaterMark: WatermarkData[] = [];
  public isLoading = true;
  public virtualItemSize = 36;
  public readonly narrowLogData = typeDefinition<WatermarkData>();

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobLocalService: JobLocalService,
    private readonly metricsService: MetricsService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
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
            catchError(() => {
              return of([] as WatermarkData[]);
            })
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
}

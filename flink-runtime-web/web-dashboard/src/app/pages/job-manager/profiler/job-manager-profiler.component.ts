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

import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Subject } from 'rxjs';
import { mergeMap, startWith, takeUntil } from 'rxjs/operators';

import {
  HumanizeWatermarkPipe,
  HumanizeWatermarkToDatetimePipe
} from '@flink-runtime-web/components/humanize-watermark.pipe';
import { ProfilingDetail } from '@flink-runtime-web/interfaces/job-profiler';
import { JobManagerService, StatusService } from '@flink-runtime-web/services';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzSpaceModule } from 'ng-zorro-antd/space';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

@Component({
  selector: 'flink-job-manager-profiler',
  templateUrl: './job-manager-profiler.component.html',
  styleUrls: ['./job-manager-profiler.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzCardModule,
    NzFormModule,
    NzInputNumberModule,
    HumanizeWatermarkPipe,
    FormsModule,
    NzButtonModule,
    NzAlertModule,
    NzTableModule,
    NzMessageModule,
    CommonModule,
    NzSpaceModule,
    HumanizeWatermarkToDatetimePipe,
    NzSelectModule,
    NzToolTipModule,
    NzIconModule
  ],
  standalone: true
})
export class JobManagerProfilerComponent implements OnInit, OnDestroy {
  private readonly destroy$ = new Subject<void>();
  profilingList: ProfilingDetail[] = [];
  isLoading = true;
  isCreating = false;
  duration = 30;
  selectMode = 'ITIMER';
  isEnabled = false;
  formatterDuration = (value: number): string => `${value} s`;
  parserDuration = (value: string): string => value.replace(' s', '');

  constructor(
    private jobManagerService: JobManagerService,
    private readonly statusService: StatusService,
    private message: NzMessageService,
    private cdr: ChangeDetectorRef
  ) {}

  public createProfilingInstance(): void {
    if (this.profilingList.length > 0 && this.profilingList[0].status === 'RUNNING') {
      this.message.warning('Please wait for last profiling finished.');
      return;
    }
    this.isCreating = true;
    this.jobManagerService.createProfilingInstance(this.selectMode, this.duration).subscribe({
      next: profilingDetail => {
        this.profilingList.unshift(profilingDetail);
        this.isCreating = false;
        this.cdr.markForCheck();
      },
      error: () => {
        this.isCreating = false;
        this.cdr.markForCheck();
      }
    });
  }

  public ngOnInit(): void {
    this.statusService.refresh$
      .pipe(
        startWith(true),
        mergeMap(() => {
          this.isLoading = true;
          this.cdr.markForCheck();
          return this.jobManagerService.loadProfilingList();
        }),
        takeUntil(this.destroy$)
      )
      .subscribe({
        next: data => {
          this.profilingList = data.profilingList;
          this.isLoading = false;
          this.isEnabled = true;
          this.cdr.markForCheck();
        },
        error: () => {
          this.isLoading = false;
          this.destroy$.next();
          this.destroy$.complete();
          this.cdr.markForCheck();
        }
      });
  }

  public downloadProfilingResult(filePath: string): void {
    this.isLoading = true;
    this.cdr.markForCheck();
    this.jobManagerService.loadProfilingResult(filePath).subscribe({
      next: data => {
        const anchor = document.createElement('a');
        anchor.href = data.url;
        anchor.download = data.url;
        document.body.appendChild(anchor);
        anchor.click();
      },
      complete: () => {
        this.isLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

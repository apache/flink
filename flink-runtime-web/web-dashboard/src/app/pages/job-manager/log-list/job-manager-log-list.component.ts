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

import { DecimalPipe, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { RouterLinkWithHref } from '@angular/router';
import { of, Subject } from 'rxjs';
import { catchError, finalize, takeUntil } from 'rxjs/operators';

import { HumanizeDatePipe } from '@flink-runtime-web/components/humanize-date.pipe';
import { JobManagerLogItem } from '@flink-runtime-web/interfaces';
import {
  JOB_MANAGER_MODULE_CONFIG,
  JOB_MANAGER_MODULE_DEFAULT_CONFIG,
  JobManagerModuleConfig
} from '@flink-runtime-web/pages/job-manager/job-manager.config';
import { JobManagerService } from '@flink-runtime-web/services';
import { NzTableModule } from 'ng-zorro-antd/table';

import { typeDefinition } from '../../../utils/strong-type';

@Component({
  selector: 'flink-job-manager-log-list',
  templateUrl: './job-manager-log-list.component.html',
  styleUrls: ['./job-manager-log-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzTableModule, NgIf, RouterLinkWithHref, HumanizeDatePipe, DecimalPipe],
  standalone: true
})
export class JobManagerLogListComponent implements OnInit, OnDestroy {
  public readonly trackByName = (_: number, log: JobManagerLogItem): string => log.name;
  public readonly narrowLogData = typeDefinition<JobManagerLogItem>();

  public readonly sortLastModifiedTimeFn = (pre: JobManagerLogItem, next: JobManagerLogItem): number =>
    pre.mtime - next.mtime;
  public readonly sortSizeFn = (pre: JobManagerLogItem, next: JobManagerLogItem): number => pre.size - next.size;

  public listOfLog: JobManagerLogItem[] = [];
  public isLoading = true;
  public logRouterFactory: (...args: string[]) => string | string[];

  private destroy$ = new Subject<void>();

  constructor(
    private readonly jobManagerService: JobManagerService,
    private readonly cdr: ChangeDetectorRef,
    @Inject(JOB_MANAGER_MODULE_CONFIG) readonly moduleConfig: JobManagerModuleConfig
  ) {
    this.logRouterFactory =
      moduleConfig.routerFactories?.jobManager || JOB_MANAGER_MODULE_DEFAULT_CONFIG.routerFactories.jobManager;
  }

  public ngOnInit(): void {
    this.jobManagerService
      .loadLogList()
      .pipe(
        catchError(() => of([] as JobManagerLogItem[])),
        finalize(() => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.listOfLog = data;
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, Input, OnDestroy, OnInit } from '@angular/core';
import { merge, Subject } from 'rxjs';
import { distinctUntilKeyChanged, takeUntil, tap } from 'rxjs/operators';

import { RouterTab } from '@flink-runtime-web/core/module-config';
import { JobDetailCorrect } from '@flink-runtime-web/interfaces';
import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { JOB_MODULE_CONFIG, JOB_MODULE_DEFAULT_CONFIG, JobModuleConfig } from '@flink-runtime-web/pages/job/job.config';
import { JobService, StatusService } from '@flink-runtime-web/services';

@Component({
  selector: 'flink-job-status',
  templateUrl: './job-status.component.html',
  styleUrls: ['./job-status.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobStatusComponent implements OnInit, OnDestroy {
  @Input() public isLoading = true;
  public statusTips: string;
  public jobDetail: JobDetailCorrect;
  public readonly listOfNavigation: RouterTab[];
  private readonly checkpointIndexOfNavigation: number;

  public webCancelEnabled = this.statusService.configuration.features['web-cancel'];

  private destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    public readonly statusService: StatusService,
    private readonly cdr: ChangeDetectorRef,
    @Inject(JOB_MODULE_CONFIG) readonly moduleConfig: JobModuleConfig
  ) {
    this.listOfNavigation = moduleConfig.routerTabs || JOB_MODULE_DEFAULT_CONFIG.routerTabs;
    this.checkpointIndexOfNavigation = this.checkpointIndexOfNav();
  }

  public ngOnInit(): void {
    const updateList$ = this.jobLocalService.jobDetailChanges().pipe(tap(data => this.handleJobDetailChanged(data)));
    const updateTip$ = this.jobLocalService.jobDetailChanges().pipe(
      distinctUntilKeyChanged('state'),
      tap(() => {
        this.statusTips = '';
        this.cdr.markForCheck();
      })
    );

    merge(updateList$, updateTip$).pipe(takeUntil(this.destroy$)).subscribe();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public cancelJob(): void {
    this.jobService.cancelJob(this.jobDetail.jid).subscribe(() => {
      this.statusTips = 'Cancelling...';
      this.cdr.markForCheck();
    });
  }

  public checkpointIndexOfNav(): number {
    return this.listOfNavigation.findIndex(item => item.path === 'checkpoints');
  }

  private handleJobDetailChanged(data: JobDetailCorrect): void {
    this.jobDetail = data;
    const index = this.checkpointIndexOfNav();
    if (data.plan.type == 'STREAMING' && index == -1) {
      this.listOfNavigation.splice(this.checkpointIndexOfNavigation, 0, {
        path: 'checkpoints',
        title: 'Checkpoints'
      });
    } else if (data.plan.type == 'BATCH' && index > -1) {
      this.listOfNavigation.splice(index, 1);
    }
    this.cdr.markForCheck();
  }
}

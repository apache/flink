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

import { DatePipe, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, Input, OnDestroy, OnInit } from '@angular/core';
import { RouterLinkWithHref } from '@angular/router';
import { merge, Subject } from 'rxjs';
import { distinctUntilKeyChanged, mergeMap, take, takeUntil, tap } from 'rxjs/operators';

import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { JobBadgeComponent } from '@flink-runtime-web/components/job-badge/job-badge.component';
import { NavigationComponent } from '@flink-runtime-web/components/navigation/navigation.component';
import { TaskBadgeComponent } from '@flink-runtime-web/components/task-badge/task-badge.component';
import { RouterTab } from '@flink-runtime-web/core/module-config';
import { JobDetailCorrect } from '@flink-runtime-web/interfaces';
import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { JOB_MODULE_CONFIG, JOB_MODULE_DEFAULT_CONFIG, JobModuleConfig } from '@flink-runtime-web/pages/job/job.config';
import { JobManagerService, JobService, StatusService } from '@flink-runtime-web/services';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';

@Component({
  selector: 'flink-job-status',
  templateUrl: './job-status.component.html',
  styleUrls: ['./job-status.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NgIf,
    NzPopconfirmModule,
    NzDescriptionsModule,
    NzDividerModule,
    JobBadgeComponent,
    TaskBadgeComponent,
    RouterLinkWithHref,
    DatePipe,
    NavigationComponent,
    NzSkeletonModule,
    HumanizeDurationPipe
  ],
  standalone: true
})
export class JobStatusComponent implements OnInit, OnDestroy {
  @Input() isLoading = true;
  statusTips: string;
  jobDetail: JobDetailCorrect;
  jmLogUrl = '';
  urlLoading = true;
  readonly listOfNavigation: RouterTab[];
  private readonly checkpointIndexOfNavigation: number;

  webCancelEnabled = this.statusService.configuration.features['web-cancel'];
  isHistoryServer = this.statusService.configuration.features['web-history'];

  private destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly jobManagerService: JobManagerService,
    private readonly statusService: StatusService,
    private readonly cdr: ChangeDetectorRef,
    @Inject(JOB_MODULE_CONFIG) readonly moduleConfig: JobModuleConfig
  ) {
    this.listOfNavigation = moduleConfig.routerTabs || JOB_MODULE_DEFAULT_CONFIG.routerTabs;
    this.checkpointIndexOfNavigation = this.checkpointIndexOfNav();
  }

  ngOnInit(): void {
    if (this.isHistoryServer) {
      this.jobLocalService
        .jobDetailChanges()
        .pipe(
          take(1),
          mergeMap(job => this.jobManagerService.loadHistoryServerJobManagerLogUrl(job.jid)),
          takeUntil(this.destroy$)
        )
        .subscribe(url => {
          this.urlLoading = false;
          this.jmLogUrl = url;
          this.cdr.markForCheck();
        });
    }

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

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  cancelJob(): void {
    this.jobService.cancelJob(this.jobDetail.jid).subscribe(() => {
      this.statusTips = 'Cancelling...';
      this.cdr.markForCheck();
    });
  }

  checkpointIndexOfNav(): number {
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

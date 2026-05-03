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
import { RouterLink } from '@angular/router';
import { merge, Subject } from 'rxjs';
import { distinctUntilKeyChanged, takeUntil, tap } from 'rxjs/operators';

import { ApplicationBadgeComponent } from '@flink-runtime-web/components/application-badge/application-badge.component';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { JobsBadgeComponent } from '@flink-runtime-web/components/jobs-badge/jobs-badge.component';
import { NavigationComponent } from '@flink-runtime-web/components/navigation/navigation.component';
import { RouterTab } from '@flink-runtime-web/core/module-config';
import { ApplicationDetail } from '@flink-runtime-web/interfaces';
import { ApplicationLocalService } from '@flink-runtime-web/pages/application/application-local.service';
import {
  APPLICATION_MODULE_CONFIG,
  APPLICATION_MODULE_DEFAULT_CONFIG,
  ApplicationModuleConfig
} from '@flink-runtime-web/pages/application/application.config';
import { ApplicationService, StatusService } from '@flink-runtime-web/services';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';

@Component({
  selector: 'flink-application-status',
  templateUrl: './application-status.component.html',
  styleUrls: ['./application-status.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NgIf,
    NzPopconfirmModule,
    NzDescriptionsModule,
    NzDividerModule,
    ApplicationBadgeComponent,
    JobsBadgeComponent,
    DatePipe,
    NavigationComponent,
    NzSkeletonModule,
    HumanizeDurationPipe,
    RouterLink
  ]
})
export class ApplicationStatusComponent implements OnInit, OnDestroy {
  @Input() isLoading = true;
  statusTips: string;
  applicationDetail: ApplicationDetail;
  readonly listOfNavigation: RouterTab[];

  webCancelEnabled = this.statusService.configuration.features['web-cancel'];
  isHistoryServer = this.statusService.configuration.features['web-history'];

  private destroy$ = new Subject<void>();

  constructor(
    private readonly applicationService: ApplicationService,
    private readonly applicationLocalService: ApplicationLocalService,
    private readonly statusService: StatusService,
    private readonly cdr: ChangeDetectorRef,
    @Inject(APPLICATION_MODULE_CONFIG) readonly moduleConfig: ApplicationModuleConfig
  ) {
    this.listOfNavigation = moduleConfig.routerTabs || APPLICATION_MODULE_DEFAULT_CONFIG.routerTabs;
  }

  ngOnInit(): void {
    const updateList$ = this.applicationLocalService.applicationDetailChanges().pipe(
      tap(data => {
        this.applicationDetail = data;
        this.cdr.markForCheck();
      })
    );
    const updateTip$ = this.applicationLocalService.applicationDetailChanges().pipe(
      distinctUntilKeyChanged('status'),
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

  cancelApplication(): void {
    this.applicationService.cancelApplication(this.applicationDetail.id).subscribe(() => {
      this.statusTips = 'Cancelling...';
      this.cdr.markForCheck();
    });
  }
}

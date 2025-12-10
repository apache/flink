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

import { ChangeDetectionStrategy, Component, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { combineLatest, Observable, Subject } from 'rxjs';
import { map, mergeMap, share, takeUntil } from 'rxjs/operators';

import { ApplicationListComponent } from '@flink-runtime-web/components/application-list/application-list.component';
import { ApplicationItem, OverviewWithApplicationStatistics } from '@flink-runtime-web/interfaces';
import { OverviewStatisticComponent } from '@flink-runtime-web/pages/overview/statistic/overview-statistic.component';
import { ApplicationService, OverviewService, StatusService } from '@flink-runtime-web/services';

@Component({
  selector: 'flink-overview',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [OverviewStatisticComponent, ApplicationListComponent]
})
export class OverviewComponent implements OnInit, OnDestroy {
  public applicationData$: Observable<ApplicationItem[]>;
  public statisticData$: Observable<OverviewWithApplicationStatistics>;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly statusService: StatusService,
    private readonly applicationService: ApplicationService,
    private readonly overviewService: OverviewService,
    private router: Router
  ) {}

  public ngOnInit(): void {
    const applications$ = this.statusService.refresh$.pipe(
      takeUntil(this.destroy$),
      mergeMap(() => this.applicationService.loadApplications()),
      share()
    );

    this.applicationData$ = applications$;
    this.statisticData$ = this.statusService.refresh$.pipe(
      takeUntil(this.destroy$),
      mergeMap(() => combineLatest([this.overviewService.loadOverview(), applications$])),
      map(([clusterOverview, applications]) => {
        const applicationStats: OverviewWithApplicationStatistics = {
          'applications-running': applications.filter(app => app.status === 'RUNNING').length,
          'applications-finished': applications.filter(app => app.status === 'FINISHED').length,
          'applications-cancelled': applications.filter(app => app.status === 'CANCELED').length,
          'applications-failed': applications.filter(app => app.status === 'FAILED').length,
          taskmanagers: clusterOverview['taskmanagers'],
          'taskmanagers-blocked': clusterOverview['taskmanagers-blocked'],
          'slots-total': clusterOverview['slots-total'],
          'slots-available': clusterOverview['slots-available'],
          'slots-free-and-blocked': clusterOverview['slots-free-and-blocked'],
          'flink-version': clusterOverview['flink-version'],
          'flink-commit': clusterOverview['flink-commit']
        };
        return applicationStats;
      }),
      share()
    );
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public navigateToApplication(commands: string[]): void {
    this.router.navigate(commands).then();
  }
}

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

import { NgFor, NgIf, JsonPipe, DatePipe, KeyValuePipe, NgTemplateOutlet } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { forkJoin, of, Subject } from 'rxjs';
import { catchError, distinctUntilChanged, switchMap, takeUntil } from 'rxjs/operators';

import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { JobBadgeComponent } from '@flink-runtime-web/components/job-badge/job-badge.component';
import {
  BriefJobRescaleDetails,
  JobRescaleDetails,
  JobRescaleConfigInfo,
  JobDetail,
  RescalesHistory,
  RescalesOverview
} from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzTooltipModule } from 'ng-zorro-antd/tooltip';

import { JobLocalService } from '../job-local.service';

@Component({
  selector: 'flink-job-rescales',
  templateUrl: './job-rescales.component.html',
  styleUrls: ['./job-rescales.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NgIf,
    NgFor,
    JsonPipe,
    DatePipe,
    KeyValuePipe,
    NgTemplateOutlet,
    NzTabsModule,
    NzDividerModule,
    JobBadgeComponent,
    HumanizeDurationPipe,
    NzTableModule,
    NzIconModule,
    NzButtonModule,
    NzEmptyModule,
    NzCollapseModule,
    NzTooltipModule
  ]
})
export class JobRescalesComponent implements OnInit, OnDestroy {
  public rescalesOverview?: RescalesOverview;
  public rescalesHistory?: RescalesHistory;
  public rescaleDetailsMap = new Map<string, JobRescaleDetails>();
  public rescalesConfig?: JobRescaleConfigInfo;
  public jobDetail: JobDetail;
  public expandedRowsSet = new Set<string>();
  public readonly Date = Date;

  private refresh$ = new Subject<void>();
  private destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.refresh$
      .pipe(
        switchMap(() =>
          forkJoin([
            this.jobService.loadRescalesOverview(this.jobDetail.jid).pipe(
              catchError(() => {
                return of(undefined);
              })
            ),
            this.jobService.loadRescalesHistory(this.jobDetail.jid).pipe(
              catchError(() => {
                return of(undefined);
              })
            ),
            this.jobService.loadRescalesConfig(this.jobDetail.jid).pipe(
              catchError(() => {
                return of(undefined);
              })
            )
          ])
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(([overview, history, config]) => {
        this.rescalesOverview = overview;
        this.rescalesHistory = history;
        this.rescalesConfig = config;

        // Load details for latest rescales in overview
        if (overview?.latest) {
          if (overview.latest.completed?.rescaleUuid) {
            this.loadRescaleDetailIfNeeded(overview.latest.completed.rescaleUuid);
          }
          if (overview.latest.failed?.rescaleUuid) {
            this.loadRescaleDetailIfNeeded(overview.latest.failed.rescaleUuid);
          }
          if (overview.latest.ignored?.rescaleUuid) {
            this.loadRescaleDetailIfNeeded(overview.latest.ignored.rescaleUuid);
          }
        }

        this.cdr.markForCheck();
      });

    this.jobLocalService
      .jobDetailChanges()
      .pipe(
        distinctUntilChanged((pre, next) => pre.jid === next.jid),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.jobDetail = data;
        this.cdr.markForCheck();
        this.refresh$.next();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.refresh$.complete();
  }

  public refresh(): void {
    const expandedUuids = Array.from(this.expandedRowsSet);

    expandedUuids.forEach(uuid => {
      this.jobService
        .loadRescaleDetail(this.jobDetail.jid, uuid)
        .pipe(takeUntil(this.destroy$))
        .subscribe(detail => {
          this.rescaleDetailsMap.set(uuid, detail);
          this.cdr.markForCheck();
        });
    });

    this.refresh$.next();
  }

  public trackById(item: BriefJobRescaleDetails): string {
    return item.rescaleUuid;
  }

  public onExpandChange(jobRescaleDetails: BriefJobRescaleDetails, expanded: boolean): void {
    if (expanded) {
      this.expandedRowsSet.add(jobRescaleDetails.rescaleUuid);
      if (!this.rescaleDetailsMap.has(jobRescaleDetails.rescaleUuid)) {
        this.jobService
          .loadRescaleDetail(this.jobDetail.jid, jobRescaleDetails.rescaleUuid)
          .pipe(takeUntil(this.destroy$))
          .subscribe(detail => {
            this.rescaleDetailsMap.set(jobRescaleDetails.rescaleUuid, detail);
            this.cdr.markForCheck();
          });
      }
    } else {
      this.expandedRowsSet.delete(jobRescaleDetails.rescaleUuid);
    }
  }

  public isExpanded(rescaleUuid: string): boolean {
    return this.expandedRowsSet.has(rescaleUuid);
  }

  public getDetail(rescaleUuid: string): JobRescaleDetails | undefined {
    return this.rescaleDetailsMap.get(rescaleUuid);
  }

  public truncateUuid(uuid: string): string {
    return uuid ? uuid.substring(0, 8) : '';
  }

  public truncateName(name: string, maxLength: number = 32): string {
    return name && name.length > maxLength ? `${name.substring(0, maxLength)}...` : name;
  }

  public getTotalRescaleCount(): number {
    if (!this.rescalesOverview?.rescalesCounts) {
      return 0;
    }
    const counts = this.rescalesOverview.rescalesCounts;
    return counts.inProgress + counts.completed + counts.failed + counts.ignored;
  }

  private loadRescaleDetailIfNeeded(rescaleUuid: string): void {
    if (!this.rescaleDetailsMap.has(rescaleUuid)) {
      this.jobService
        .loadRescaleDetail(this.jobDetail.jid, rescaleUuid)
        .pipe(takeUntil(this.destroy$))
        .subscribe(detail => {
          this.rescaleDetailsMap.set(rescaleUuid, detail);
          this.cdr.markForCheck();
        });
    }
  }
}

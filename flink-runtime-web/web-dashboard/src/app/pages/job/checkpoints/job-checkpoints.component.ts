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

import { DatePipe, NgForOf, NgIf, PercentPipe } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { forkJoin, of, Subject } from 'rxjs';
import { catchError, distinctUntilChanged, switchMap, takeUntil } from 'rxjs/operators';

import { CheckpointBadgeComponent } from '@flink-runtime-web/components/checkpoint-badge/checkpoint-badge.component';
import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { CheckpointConfig, CheckpointHistory, Checkpoint, JobDetailCorrect } from '@flink-runtime-web/interfaces';
import { JobCheckpointsDetailComponent } from '@flink-runtime-web/pages/job/checkpoints/detail/job-checkpoints-detail.component';
import { JobService } from '@flink-runtime-web/services';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

import { JobLocalService } from '../job-local.service';

@Component({
  selector: 'flink-job-checkpoints',
  templateUrl: './job-checkpoints.component.html',
  styleUrls: ['./job-checkpoints.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NgIf,
    NzTabsModule,
    NzDividerModule,
    DatePipe,
    HumanizeDurationPipe,
    JobCheckpointsDetailComponent,
    HumanizeBytesPipe,
    NzTableModule,
    NzIconModule,
    NgForOf,
    CheckpointBadgeComponent,
    PercentPipe,
    NzButtonModule,
    NzEmptyModule,
    NzCollapseModule,
    NzToolTipModule
  ],
  standalone: true
})
export class JobCheckpointsComponent implements OnInit, OnDestroy {
  disabledInterval = 0x7fffffffffffffff;

  public readonly trackById = (_: number, node: CheckpointHistory): number => node.id;

  public checkPointStats?: Checkpoint;
  public checkPointConfig?: CheckpointConfig;
  public jobDetail: JobDetailCorrect;

  public moreDetailsPanel = { active: false, disabled: false };

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
            this.jobService.loadCheckpointStats(this.jobDetail.jid).pipe(
              catchError(() => {
                return of(undefined);
              })
            ),
            this.jobService.loadCheckpointConfig(this.jobDetail.jid).pipe(
              catchError(() => {
                return of(undefined);
              })
            )
          ])
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(([stats, config]) => {
        this.checkPointStats = stats;
        this.checkPointConfig = config;
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
    this.refresh$.next();
  }
}

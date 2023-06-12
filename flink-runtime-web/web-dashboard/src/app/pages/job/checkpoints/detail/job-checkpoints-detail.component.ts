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
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';
import { forkJoin, Subject } from 'rxjs';
import { first, takeUntil } from 'rxjs/operators';

import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import {
  CheckpointCompletedStatistics,
  CheckpointDetail,
  JobDetailCorrect,
  VerticesItem,
  CheckpointConfig
} from '@flink-runtime-web/interfaces';
import { JobCheckpointsSubtaskComponent } from '@flink-runtime-web/pages/job/checkpoints/subtask/job-checkpoints-subtask.component';
import { JobService } from '@flink-runtime-web/services';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzTableModule } from 'ng-zorro-antd/table';

import { JobLocalService } from '../../job-local.service';

@Component({
  selector: 'flink-job-checkpoints-detail',
  templateUrl: './job-checkpoints-detail.component.html',
  styleUrls: ['./job-checkpoints-detail.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzDividerModule,
    NzTableModule,
    NgIf,
    NgForOf,
    PercentPipe,
    HumanizeDurationPipe,
    HumanizeBytesPipe,
    JobCheckpointsSubtaskComponent,
    DatePipe
  ],
  standalone: true
})
export class JobCheckpointsDetailComponent implements OnInit, OnDestroy {
  public readonly trackById = (_: number, node: VerticesItem): string => node.id;

  public innerCheckPoint: CheckpointCompletedStatistics;
  public jobDetail: JobDetailCorrect;
  public checkPointType: string;

  public checkPointDetail: CheckpointDetail;
  public checkPointConfig: CheckpointConfig;
  public listOfVertex: VerticesItem[] = [];
  public isLoading = true;

  @Input()
  public set checkPoint(value) {
    this.innerCheckPoint = value;
    this.refresh();
  }

  public get checkPoint(): CheckpointCompletedStatistics {
    return this.innerCheckPoint;
  }

  private destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.jobLocalService
      .jobDetailChanges()
      .pipe(first(), takeUntil(this.destroy$))
      .subscribe(data => {
        this.jobDetail = data;
        this.listOfVertex = data!.vertices;
        this.cdr.markForCheck();
        this.refresh();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public refresh(): void {
    this.isLoading = true;
    if (this.jobDetail && this.jobDetail.jid) {
      forkJoin([
        this.jobService.loadCheckpointConfig(this.jobDetail.jid),
        this.jobService.loadCheckpointDetails(this.jobDetail.jid, this.checkPoint.id)
      ])
        .pipe(takeUntil(this.destroy$))
        .subscribe(
          ([config, detail]) => {
            this.checkPointConfig = config;
            this.checkPointDetail = detail;
            if (this.checkPointDetail.checkpoint_type === 'CHECKPOINT') {
              this.checkPointType = 'aligned checkpoint';
            } else if (this.checkPointDetail.checkpoint_type === 'UNALIGNED_CHECKPOINT') {
              this.checkPointType = 'unaligned checkpoint';
            } else if (this.checkPointDetail.checkpoint_type === 'SYNC_SAVEPOINT') {
              this.checkPointType = 'savepoint on cancel';
            } else if (this.checkPointDetail.checkpoint_type === 'SAVEPOINT') {
              this.checkPointType = 'savepoint';
            } else {
              this.checkPointType = '-';
            }
            this.isLoading = false;
            this.cdr.markForCheck();
          },
          () => {
            this.isLoading = false;
            this.cdr.markForCheck();
          }
        );
    }
  }
}

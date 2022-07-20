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

import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  SimpleChanges
} from '@angular/core';
import { Subject } from 'rxjs';
import { first, takeUntil } from 'rxjs/operators';

import {
  CheckpointSubTask,
  CompletedSubTaskCheckpointStatistics,
  JobDetailCorrect,
  SubTaskCheckpointStatisticsItem,
  VerticesItem
} from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';

import { JobLocalService } from '../../job-local.service';

function createSortFn(
  selector: (item: CompletedSubTaskCheckpointStatistics) => number | boolean
): NzTableSortFn<SubTaskCheckpointStatisticsItem> {
  // FIXME This type-asserts that pre / next are a specific subtype.
  return (pre, next) =>
    selector(pre as CompletedSubTaskCheckpointStatistics) > selector(next as CompletedSubTaskCheckpointStatistics)
      ? 1
      : -1;
}

@Component({
  selector: 'flink-job-checkpoints-subtask',
  templateUrl: './job-checkpoints-subtask.component.html',
  styleUrls: ['./job-checkpoints-subtask.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobCheckpointsSubtaskComponent implements OnInit, OnChanges, OnDestroy {
  @Input() public vertex: VerticesItem;
  @Input() public checkPointId: number;

  public jobDetail: JobDetailCorrect;
  public subTaskCheckPoint: CheckpointSubTask;
  public listOfSubTaskCheckPoint: SubTaskCheckpointStatisticsItem[] = [];
  public isLoading = true;
  public sortName: string;
  public sortValue: string;

  public readonly sortAckTimestampFn = createSortFn(item => item.ack_timestamp);
  public readonly sortEndToEndDurationFn = createSortFn(item => item.end_to_end_duration);
  public readonly sortCheckpointedSizeFn = createSortFn(item => item.checkpointed_size);
  public readonly sortStateSizeFn = createSortFn(item => item.state_size);
  public readonly sortCpSyncFn = createSortFn(item => item.checkpoint?.sync);
  public readonly sortCpAsyncFn = createSortFn(item => item.checkpoint?.async);
  public readonly sortAlignmentProcessedFn = createSortFn(item => item.alignment?.processed);
  public readonly sortAlignmentDurationFn = createSortFn(item => item.alignment?.duration);
  public readonly sortStartDelayFn = createSortFn(item => item.start_delay);
  public readonly sortUnalignedCpFn = createSortFn(item => item.unaligned_checkpoint);

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
      .subscribe(job => {
        this.jobDetail = job;
        this.refresh();
      });
  }

  public ngOnChanges(changes: SimpleChanges): void {
    if (changes.checkPointId) {
      this.refresh();
    }
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public refresh(): void {
    if (this.jobDetail && this.jobDetail.jid) {
      this.jobService
        .loadCheckpointSubtaskDetails(this.jobDetail.jid, this.checkPointId, this.vertex.id)
        .pipe(takeUntil(this.destroy$))
        .subscribe(
          data => {
            this.subTaskCheckPoint = data;
            this.listOfSubTaskCheckPoint = (data && data.subtasks) || [];
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

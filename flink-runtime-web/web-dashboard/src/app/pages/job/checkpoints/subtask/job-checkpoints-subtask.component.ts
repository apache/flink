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
  OnInit,
  SimpleChanges
} from '@angular/core';
import { CheckPointSubTaskInterface, JobDetailCorrectInterface, VerticesItemInterface } from 'interfaces';
import { first } from 'rxjs/operators';
import { JobService } from 'services';
import { deepFind } from 'utils';
import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';

@Component({
  selector: 'flink-job-checkpoints-subtask',
  templateUrl: './job-checkpoints-subtask.component.html',
  styleUrls: ['./job-checkpoints-subtask.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobCheckpointsSubtaskComponent implements OnInit, OnChanges {
  @Input() vertex: VerticesItemInterface;
  @Input() checkPointId: number;
  jobDetail: JobDetailCorrectInterface;
  subTaskCheckPoint: CheckPointSubTaskInterface;
  listOfSubTaskCheckPoint: Array<{ index: number; status: string }> = [];
  isLoading = true;
  sortName: string;
  sortValue: string;

  sortAckTimestampFn = this.sortFn('ack_timestamp');
  sortEndToEndDurationFn = this.sortFn('end_to_end_duration');
  sortStateSizeFn = this.sortFn('state_size');
  sortCpSyncFn = this.sortFn('checkpoint.sync');
  sortCpAsyncFn = this.sortFn('checkpoint.async');
  sortAlignmentProcessedFn = this.sortFn('alignment.processed');
  sortAlignmentDurationFn = this.sortFn('alignment.duration');
  sortStartDelayFn = this.sortFn('start_delay');
  sortUnalignedCpFn = this.sortFn('unaligned_checkpoint');

  sortFn(path: string): NzTableSortFn<{ index: number; status: string }> {
    return (pre: { index: number; status: string }, next: { index: number; status: string }) =>
      deepFind(pre, path) > deepFind(next, path) ? 1 : -1;
  }

  refresh() {
    if (this.jobDetail && this.jobDetail.jid) {
      this.jobService.loadCheckpointSubtaskDetails(this.jobDetail.jid, this.checkPointId, this.vertex.id).subscribe(
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

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.jobService.jobDetail$.pipe(first()).subscribe(job => {
      this.jobDetail = job;
      this.refresh();
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.checkPointId) {
      this.refresh();
    }
  }
}

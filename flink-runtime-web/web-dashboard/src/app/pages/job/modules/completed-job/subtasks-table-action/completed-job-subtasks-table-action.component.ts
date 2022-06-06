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

import { Component, ChangeDetectionStrategy, ChangeDetectorRef, Input, OnInit, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { mergeMap, take, takeUntil } from 'rxjs/operators';

import { JobVertexStatusDuration, JobVertexSubTask } from '@flink-runtime-web/interfaces';
import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { JobOverviewSubtasksTableAction } from '@flink-runtime-web/pages/job/overview/subtasks/table-action/subtasks-table-action.component';
import { StatusService, TaskManagerService } from '@flink-runtime-web/services';

@Component({
  selector: 'flink-completed-job-subtasks-table-action',
  templateUrl: './completed-job-subtasks-table-action.component.html',
  styleUrls: ['./completed-job-subtasks-table-action.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class CompletedJobSubtasksTableActionComponent implements OnInit, OnDestroy, JobOverviewSubtasksTableAction {
  @Input() subtask?: JobVertexSubTask;
  statusDuration: Array<{ key: string; value: number }> = [];
  isHistoryServer = false;
  visible = false;
  loading = true;
  logUrl = '';

  private destroy$ = new Subject<void>();

  constructor(
    private statusService: StatusService,
    private jobLocalService: JobLocalService,
    private taskManagerService: TaskManagerService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.isHistoryServer = this.statusService.configuration.features['web-history'];
    this.cdr.markForCheck();

    if (this.subtask) {
      this.statusDuration = this.convertStatusDuration(this.subtask['status-duration']);
      if (this.isHistoryServer) {
        this.jobLocalService
          .jobDetailChanges()
          .pipe(
            take(1),
            mergeMap(job =>
              this.taskManagerService.loadHistoryServerTaskManagerLogUrl(job.jid, this.subtask!['taskmanager-id'])
            ),
            takeUntil(this.destroy$)
          )
          .subscribe(url => {
            this.loading = false;
            this.logUrl = url;
            this.cdr.markForCheck();
          });
      }
    }
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  setModalVisible(visible: boolean): void {
    this.visible = visible;
    this.cdr.markForCheck();
  }

  convertStatusDuration(duration: JobVertexStatusDuration<number>): Array<{ key: string; value: number }> {
    return Object.keys(duration).map(key => ({
      key,
      value: duration[key as keyof JobVertexStatusDuration<number>]
    }));
  }
}

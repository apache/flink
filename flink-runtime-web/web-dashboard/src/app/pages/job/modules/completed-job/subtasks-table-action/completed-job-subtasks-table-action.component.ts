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

import { NgIf } from '@angular/common';
import { Component, ChangeDetectionStrategy, ChangeDetectorRef, Input, OnInit, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { filter, mergeMap, take, takeUntil } from 'rxjs/operators';

import { JobVertexSubTask } from '@flink-runtime-web/interfaces';
import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { JobOverviewSubtasksTableAction } from '@flink-runtime-web/pages/job/overview/subtasks/table-action/subtasks-table-action.component';
import { TaskManagerService } from '@flink-runtime-web/services';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzIconModule } from 'ng-zorro-antd/icon';

@Component({
  selector: 'flink-completed-job-subtasks-table-action',
  templateUrl: './completed-job-subtasks-table-action.component.html',
  styleUrls: ['./completed-job-subtasks-table-action.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf, NzDropDownModule, NzIconModule],
  standalone: true
})
export class CompletedJobSubtasksTableActionComponent implements OnInit, OnDestroy, JobOverviewSubtasksTableAction {
  @Input() subtask?: JobVertexSubTask;
  loading = true;
  logUrl = '';

  private destroy$ = new Subject<void>();

  constructor(
    private jobLocalService: JobLocalService,
    private taskManagerService: TaskManagerService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.jobLocalService
      .jobDetailChanges()
      .pipe(
        take(1),
        filter(() => !!this.subtask?.['taskmanager-id'] && this.subtask['taskmanager-id'] !== '(unassigned)'),
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

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

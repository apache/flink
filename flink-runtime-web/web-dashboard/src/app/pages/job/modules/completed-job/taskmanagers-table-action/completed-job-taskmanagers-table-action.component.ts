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
import { Component, ChangeDetectionStrategy, Input, ChangeDetectorRef, OnInit, OnDestroy } from '@angular/core';
import { Subject } from 'rxjs';
import { filter, mergeMap, take, takeUntil } from 'rxjs/operators';

import { TableAggregatedMetricsComponent } from '@flink-runtime-web/components/table-aggregated-metrics/table-aggregated-metrics.component';
import { VertexTaskManagerDetail } from '@flink-runtime-web/interfaces';
import { JobLocalService } from '@flink-runtime-web/pages/job/job-local.service';
import { JobOverviewTaskManagersTableAction } from '@flink-runtime-web/pages/job/overview/taskmanagers/table-action/taskmanagers-table-action.component';
import { TaskManagerService } from '@flink-runtime-web/services';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzModalModule } from 'ng-zorro-antd/modal';

@Component({
  selector: 'flink-completed-job-taskmanagers-table-action',
  templateUrl: './completed-job-taskmanagers-table-action.component.html',
  styleUrls: ['./completed-job-taskmanagers-table-action.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NgIf, NzDropDownModule, NzIconModule, NzModalModule, TableAggregatedMetricsComponent],
  standalone: true
})
export class CompletedJobTaskmanagersTableActionComponent
  implements OnInit, OnDestroy, JobOverviewTaskManagersTableAction
{
  @Input() taskManager?: VertexTaskManagerDetail;
  visible = false;
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
        filter(() => !!this.taskManager?.['taskmanager-id'] && this.taskManager['taskmanager-id'] !== '(unassigned)'),
        mergeMap(job =>
          this.taskManagerService.loadHistoryServerTaskManagerLogUrl(job.jid, this.taskManager!['taskmanager-id'])
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

  setModalVisible(visible: boolean): void {
    this.visible = visible;
    this.cdr.markForCheck();
  }
}

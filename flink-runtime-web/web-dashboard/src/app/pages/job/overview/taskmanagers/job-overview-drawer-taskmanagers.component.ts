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

import { DecimalPipe, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit, Type } from '@angular/core';
import { of, Subject } from 'rxjs';
import { catchError, map, mergeMap, takeUntil } from 'rxjs/operators';

import { DynamicHostComponent } from '@flink-runtime-web/components/dynamic/dynamic-host.component';
import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { HumanizeDatePipe } from '@flink-runtime-web/components/humanize-date.pipe';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { VertexTaskManagerDetail } from '@flink-runtime-web/interfaces';
import {
  JOB_OVERVIEW_MODULE_CONFIG,
  JOB_OVERVIEW_MODULE_DEFAULT_CONFIG,
  JobOverviewModuleConfig
} from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { JobService } from '@flink-runtime-web/services';
import { typeDefinition } from '@flink-runtime-web/utils/strong-type';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';

import { JobLocalService } from '../../job-local.service';

function createSortFn(
  selector: (item: VertexTaskManagerDetail) => number | string
): NzTableSortFn<VertexTaskManagerDetail> {
  return (pre, next) => (selector(pre) > selector(next) ? 1 : -1);
}

@Component({
  selector: 'flink-job-overview-drawer-taskmanagers',
  templateUrl: './job-overview-drawer-taskmanagers.component.html',
  styleUrls: ['./job-overview-drawer-taskmanagers.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzTableModule,
    NgIf,
    HumanizeBytesPipe,
    DecimalPipe,
    HumanizeDatePipe,
    HumanizeDurationPipe,
    DynamicHostComponent
  ],
  standalone: true
})
export class JobOverviewDrawerTaskmanagersComponent implements OnInit, OnDestroy {
  public readonly trackByHost = (_: number, node: VertexTaskManagerDetail): string => node.host;

  public readonly sortReadBytesFn = createSortFn(item => item.metrics?.['read-bytes']);
  public readonly sortReadRecordsFn = createSortFn(item => item.metrics?.['read-records']);
  public readonly sortWriteBytesFn = createSortFn(item => item.metrics?.['write-bytes']);
  public readonly sortWriteRecordsFn = createSortFn(item => item.metrics?.['write-records']);
  public readonly sortHostFn = createSortFn(item => item.host);
  public readonly sortStartTimeFn = createSortFn(item => item['start-time']);
  public readonly sortDurationFn = createSortFn(item => item.duration);
  public readonly sortEndTimeFn = createSortFn(item => item['end-time']);
  public readonly sortStatusFn = createSortFn(item => item.status);

  public listOfTaskManager: VertexTaskManagerDetail[] = [];
  public isLoading = true;
  public virtualItemSize = 36;
  public actionComponent: Type<unknown>;
  public taskCountBadgeComponent: Type<unknown>;
  public stateBadgeComponent: Type<unknown>;
  public readonly narrowLogData = typeDefinition<VertexTaskManagerDetail>();

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef,
    @Inject(JOB_OVERVIEW_MODULE_CONFIG) readonly moduleConfig: JobOverviewModuleConfig
  ) {
    this.actionComponent =
      moduleConfig.customComponents?.taskManagerActionComponent ||
      JOB_OVERVIEW_MODULE_DEFAULT_CONFIG.customComponents.taskManagerActionComponent;
    this.taskCountBadgeComponent =
      moduleConfig.customComponents?.taskCountBadgeComponent ||
      JOB_OVERVIEW_MODULE_DEFAULT_CONFIG.customComponents.taskCountBadgeComponent;
    this.stateBadgeComponent =
      moduleConfig.customComponents?.stateBadgeComponent ||
      JOB_OVERVIEW_MODULE_DEFAULT_CONFIG.customComponents.stateBadgeComponent;
  }

  ngOnInit(): void {
    this.jobLocalService
      .jobWithVertexChanges()
      .pipe(
        mergeMap(data =>
          this.jobService.loadTaskManagers(data.job.jid, data.vertex!.id).pipe(
            map(data => data.taskmanagers),
            catchError(() => of([]))
          )
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(taskmanagers => {
        this.listOfTaskManager = taskmanagers;
        this.isLoading = false;
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

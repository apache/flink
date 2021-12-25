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

import { ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { Subject } from 'rxjs';
import { mergeMap, takeUntil } from 'rxjs/operators';

import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';

import { VertexTaskManagerDetail } from 'interfaces';
import { JobService } from 'services';

function createSortFn(
  selector: (item: VertexTaskManagerDetail) => number | string
): NzTableSortFn<VertexTaskManagerDetail> {
  return (pre, next) => (selector(pre) > selector(next) ? 1 : -1);
}

@Component({
  selector: 'flink-job-overview-drawer-taskmanagers',
  templateUrl: './job-overview-drawer-taskmanagers.component.html',
  styleUrls: ['./job-overview-drawer-taskmanagers.component.less']
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
  public sortName: string;
  public sortValue: string;
  public isLoading = true;

  private readonly destroy$ = new Subject<void>();

  constructor(private readonly jobService: JobService, private readonly cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.jobService.jobWithVertex$
      .pipe(
        takeUntil(this.destroy$),
        mergeMap(data => this.jobService.loadTaskManagers(data.job.jid, data.vertex!.id))
      )
      .subscribe(
        data => {
          this.listOfTaskManager = data.taskmanagers;
          this.isLoading = false;
          this.cdr.markForCheck();
        },
        () => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }
      );
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

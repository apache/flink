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
import { flatMap, takeUntil } from 'rxjs/operators';
import { deepFind } from 'utils';
import { VertexTaskManagerDetailInterface } from 'interfaces';
import { JobService } from 'services';
import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';

@Component({
  selector: 'flink-job-overview-drawer-taskmanagers',
  templateUrl: './job-overview-drawer-taskmanagers.component.html',
  styleUrls: ['./job-overview-drawer-taskmanagers.component.less']
})
export class JobOverviewDrawerTaskmanagersComponent implements OnInit, OnDestroy {
  listOfTaskManager: VertexTaskManagerDetailInterface[] = [];
  destroy$ = new Subject();
  sortName: string;
  sortValue: string;
  isLoading = true;

  sortReadBytesFn = this.sortFn('metrics.read-bytes');
  sortReadRecordsFn = this.sortFn('metrics.read-records');
  sortWriteBytesFn = this.sortFn('metrics.write-bytes');
  sortWriteRecordsFn = this.sortFn('metrics.write-records');
  sortAttemptFn = this.sortFn('attempt');
  sortHostFn = this.sortFn('host');
  sortStartTimeFn = this.sortFn('detail.start-time');
  sortDurationFn = this.sortFn('detail.duration');
  sortEndTimeFn = this.sortFn('detail.end-time');
  sortStatusFn = this.sortFn('status');

  sortFn(path: string): NzTableSortFn<VertexTaskManagerDetailInterface> {
    return (pre: VertexTaskManagerDetailInterface, next: VertexTaskManagerDetailInterface) =>
      deepFind(pre, path) > deepFind(next, path) ? 1 : -1;
  }

  trackTaskManagerBy(_: number, node: VertexTaskManagerDetailInterface) {
    return node.host;
  }

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.jobService.jobWithVertex$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(data => this.jobService.loadTaskManagers(data.job.jid, data.vertex!.id))
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

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

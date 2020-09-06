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

  trackTaskManagerBy(_: number, node: VertexTaskManagerDetailInterface) {
    return node.host;
  }

  sort(sort: { key: string; value: string }) {
    this.sortName = sort.key;
    this.sortValue = sort.value;
    this.search();
  }

  search() {
    if (this.sortName) {
      this.listOfTaskManager = [
        ...this.listOfTaskManager.sort((pre, next) => {
          if (this.sortValue === 'ascend') {
            return deepFind(pre, this.sortName) > deepFind(next, this.sortName) ? 1 : -1;
          } else {
            return deepFind(next, this.sortName) > deepFind(pre, this.sortName) ? 1 : -1;
          }
        })
      ];
    }
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
          this.search();
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

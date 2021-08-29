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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { TaskmanagersItemInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { StatusService, TaskManagerService } from 'services';
import { deepFind } from 'utils';
import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';

@Component({
  selector: 'flink-task-manager-list',
  templateUrl: './task-manager-list.component.html',
  styleUrls: ['./task-manager-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerListComponent implements OnInit, OnDestroy {
  listOfTaskManager: TaskmanagersItemInterface[] = [];
  isLoading = true;
  destroy$ = new Subject();
  sortName: string;
  sortValue: string;

  sortDataPortFn = this.sortFn('dataPort');
  sortHeartBeatFn = this.sortFn('timeSinceLastHeartbeat');
  sortSlotsNumberFn = this.sortFn('slotsNumber');
  sortFreeSlotsFn = this.sortFn('freeSlots');
  sortCpuCoresFn = this.sortFn('hardware.cpuCores');
  sortPhysicalMemoryFn = this.sortFn('hardware.physicalMemory');
  sortFreeMemoryFn = this.sortFn('hardware.freeMemory');
  sortManagedMemoryFn = this.sortFn('hardware.managedMemory');

  sortFn(path: string): NzTableSortFn<TaskmanagersItemInterface> {
    return (pre: TaskmanagersItemInterface, next: TaskmanagersItemInterface) =>
      deepFind(pre, path) > deepFind(next, path) ? 1 : -1;
  }

  trackManagerBy(_: number, node: TaskmanagersItemInterface) {
    return node.id;
  }

  navigateTo(taskManager: TaskmanagersItemInterface) {
    this.router.navigate([taskManager.id, 'metrics'], { relativeTo: this.activatedRoute }).then();
  }

  constructor(
    private cdr: ChangeDetectorRef,
    private statusService: StatusService,
    private taskManagerService: TaskManagerService,
    private router: Router,
    private activatedRoute: ActivatedRoute
  ) {}

  ngOnInit() {
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(() => this.taskManagerService.loadManagers())
      )
      .subscribe(
        data => {
          this.isLoading = false;
          this.listOfTaskManager = data;
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

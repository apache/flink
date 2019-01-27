/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { deepFind } from 'flink-core';
import { Subject } from 'rxjs';
import { first, flatMap, takeUntil } from 'rxjs/operators';
import { StatusService, TaskManagerService } from 'flink-services';

@Component({
  selector   : 'flink-task-manager-list',
  templateUrl: './task-manager-list.component.html',
  styleUrls  : [ './task-manager-list.component.less' ]
})
export class TaskManagerListComponent implements OnInit, OnDestroy {
  listOfTaskManager = [];
  isLoading = true;
  destroy$ = new Subject();
  sortName = null;
  sortValue = null;

  sort(sort: { key: string, value: string }) {
    this.sortName = sort.key;
    this.sortValue = sort.value;
    this.search();
  }

  search() {
    if (this.sortName) {
      this.listOfTaskManager = [ ...this.listOfTaskManager.sort(
        (pre, next) => {
          if (this.sortValue === 'ascend') {
            return (deepFind(pre, this.sortName) > deepFind(next, this.sortName) ? 1 : -1);
          } else {
            return (deepFind(next, this.sortName) > deepFind(pre, this.sortName) ? 1 : -1);
          }
        }) ];
    }
  }

  trackManagerBy(index, node) {
    return node.id;
  }

  navigateTo(taskManager) {
    this.router.navigate([ taskManager.id, 'resource' ], { relativeTo: this.activatedRoute }).then();
  }

  constructor(
    private statusService: StatusService,
    private taskManagerService: TaskManagerService,
    private router: Router,
    private activatedRoute: ActivatedRoute) {
  }

  ngOnInit() {
    this.statusService.refresh$.pipe(
      first(),
      takeUntil(this.destroy$),
      flatMap(() => this.taskManagerService.loadManagers())
    ).subscribe(data => {
      this.isLoading = false;
      this.listOfTaskManager = data;
    }, () => {
      this.isLoading = false;
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

}

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
import { ActivatedRoute } from '@angular/router';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { StatusService, TaskManagerService } from 'flink-services';

@Component({
  selector   : 'flink-task-manager',
  templateUrl: './task-manager.component.html',
  styleUrls  : [ './task-manager.component.less' ]
})
export class TaskManagerComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  isLoading = true;

  constructor(
    private activatedRoute: ActivatedRoute,
    private taskManagerService: TaskManagerService,
    private statusService: StatusService) {
  }

  ngOnInit() {
    this.statusService.refresh$.pipe(
      takeUntil(this.destroy$),
      flatMap(() => this.taskManagerService.loadManager(this.activatedRoute.snapshot.params.taskManagerId))
    ).subscribe(data => {
      this.taskManagerService.taskManagerDetail = data;
      this.taskManagerService.taskManagerDetail$.next(data);
      this.isLoading = false;
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    this.taskManagerService.taskManagerDetail = null;
  }
}

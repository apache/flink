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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';
import { TaskManagerDetailInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { TaskManagerService } from 'services';

@Component({
  selector: 'flink-task-manager-status',
  templateUrl: './task-manager-status.component.html',
  styleUrls: ['./task-manager-status.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerStatusComponent implements OnInit, OnDestroy {
  @Input() isLoading = true;
  listOfNavigation = [
    { path: 'metrics', title: 'Metrics' },
    { path: 'logs', title: 'Logs' },
    { path: 'stdout', title: 'Stdout' },
    { path: 'log-list', title: 'Log List' },
    { path: 'thread-dump', title: 'Thread Dump' }
  ];
  taskManagerDetail: TaskManagerDetailInterface;
  private destroy$ = new Subject();

  constructor(private taskManagerService: TaskManagerService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.taskManagerService.taskManagerDetail$.pipe(takeUntil(this.destroy$)).subscribe(data => {
      this.taskManagerDetail = data;
      this.cdr.markForCheck();
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

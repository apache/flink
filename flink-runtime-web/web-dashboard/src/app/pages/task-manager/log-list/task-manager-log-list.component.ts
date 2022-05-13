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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { Subject } from 'rxjs';
import { first, mergeMap, takeUntil } from 'rxjs/operators';

import { TaskManagerLogItem } from '@flink-runtime-web/interfaces';
import { TaskManagerService } from '@flink-runtime-web/services';

import { typeDefinition } from '../../../utils/strong-type';
import { TaskManagerLocalService } from '../task-manager-local.service';

@Component({
  selector: 'flink-task-manager-log-list',
  templateUrl: './task-manager-log-list.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerLogListComponent implements OnInit, OnDestroy {
  public readonly trackByName = (_: number, log: TaskManagerLogItem): string => log.name;
  public readonly narrowLogData = typeDefinition<TaskManagerLogItem>();

  public readonly sortLastModifiedTimeFn = (pre: TaskManagerLogItem, next: TaskManagerLogItem): number =>
    pre.mtime - next.mtime;
  public readonly sortSizeFn = (pre: TaskManagerLogItem, next: TaskManagerLogItem): number => pre.size - next.size;

  public listOfLog: TaskManagerLogItem[] = [];
  public isLoading = true;

  private destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly taskManagerLocalService: TaskManagerLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.taskManagerLocalService
      .taskManagerDetailChanges()
      .pipe(
        first(),
        mergeMap(data => this.taskManagerService.loadLogList(data.id)),
        takeUntil(this.destroy$)
      )
      .subscribe(
        data => {
          this.listOfLog = data;
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

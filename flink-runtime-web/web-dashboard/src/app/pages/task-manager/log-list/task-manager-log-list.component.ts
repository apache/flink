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

import { DecimalPipe, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, RouterLinkWithHref } from '@angular/router';
import { of, Subject } from 'rxjs';
import { catchError, takeUntil } from 'rxjs/operators';

import { HumanizeDatePipe } from '@flink-runtime-web/components/humanize-date.pipe';
import { TaskManagerLogItem } from '@flink-runtime-web/interfaces';
import {
  TASK_MANAGER_MODULE_CONFIG,
  TASK_MANAGER_MODULE_DEFAULT_CONFIG,
  TaskManagerModuleConfig
} from '@flink-runtime-web/pages/task-manager/task-manager.config';
import { TaskManagerService } from '@flink-runtime-web/services';
import { NzTableModule } from 'ng-zorro-antd/table';

import { typeDefinition } from '../../../utils/strong-type';

@Component({
  selector: 'flink-task-manager-log-list',
  templateUrl: './task-manager-log-list.component.html',
  styleUrls: ['./task-manager-log-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzTableModule, NgIf, RouterLinkWithHref, HumanizeDatePipe, DecimalPipe],
  standalone: true
})
export class TaskManagerLogListComponent implements OnInit, OnDestroy {
  public readonly trackByName = (_: number, log: TaskManagerLogItem): string => log.name;
  public readonly narrowLogData = typeDefinition<TaskManagerLogItem>();

  public readonly sortLastModifiedTimeFn = (pre: TaskManagerLogItem, next: TaskManagerLogItem): number =>
    pre.mtime - next.mtime;
  public readonly sortSizeFn = (pre: TaskManagerLogItem, next: TaskManagerLogItem): number => pre.size - next.size;

  public listOfLog: TaskManagerLogItem[] = [];
  public isLoading = true;
  public logRouterFactory: (...args: string[]) => string | string[];

  private destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly activatedRoute: ActivatedRoute,
    private readonly cdr: ChangeDetectorRef,
    @Inject(TASK_MANAGER_MODULE_CONFIG) readonly moduleConfig: TaskManagerModuleConfig
  ) {
    this.logRouterFactory =
      moduleConfig.routerFactories?.taskManager || TASK_MANAGER_MODULE_DEFAULT_CONFIG.routerFactories.taskManager;
  }

  public ngOnInit(): void {
    const taskManagerId = this.activatedRoute.parent!.snapshot.params.taskManagerId;
    this.taskManagerService
      .loadLogList(taskManagerId)
      .pipe(
        catchError(() => of([] as TaskManagerLogItem[])),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.listOfLog = data;
        this.isLoading = false;
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

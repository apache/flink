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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { first, mergeMap } from 'rxjs/operators';

import { TaskManagerLogItem } from 'interfaces';
import { TaskManagerService } from 'services';

import { typeDefinition } from '../../../utils/strong-type';

@Component({
  selector: 'flink-task-manager-log-list',
  templateUrl: './task-manager-log-list.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerLogListComponent implements OnInit {
  public readonly trackByName = (_: number, log: TaskManagerLogItem): string => log.name;
  public readonly narrowLogData = typeDefinition<TaskManagerLogItem>();

  public readonly sortLastModifiedTimeFn = (pre: TaskManagerLogItem, next: TaskManagerLogItem): number =>
    pre.mtime - next.mtime;
  public readonly sortSizeFn = (pre: TaskManagerLogItem, next: TaskManagerLogItem): number => pre.size - next.size;

  public listOfLog: TaskManagerLogItem[] = [];
  public isLoading = true;

  constructor(private readonly taskManagerService: TaskManagerService, private readonly cdr: ChangeDetectorRef) {}

  public ngOnInit(): void {
    this.taskManagerService.taskManagerDetail$
      .pipe(
        first(),
        mergeMap(data => this.taskManagerService.loadLogList(data.id))
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
}

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
import { first, flatMap } from 'rxjs/operators';

import { TaskManagerLogItemInterface } from 'interfaces';
import { TaskManagerService } from 'services';

import { typeDefinition } from '../../../utils/strong-type';

@Component({
  selector: 'flink-task-manager-log-list',
  templateUrl: './task-manager-log-list.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerLogListComponent implements OnInit {
  listOfLog: TaskManagerLogItemInterface[] = [];
  isLoading = true;

  trackByName = (_: number, log: TaskManagerLogItemInterface): string => log.name;
  readonly narrowLogData = typeDefinition<TaskManagerLogItemInterface>();

  sortLastModifiedTimeFn = (pre: TaskManagerLogItemInterface, next: TaskManagerLogItemInterface): number =>
    pre.mtime - next.mtime;
  sortSizeFn = (pre: TaskManagerLogItemInterface, next: TaskManagerLogItemInterface): number => pre.size - next.size;

  constructor(private taskManagerService: TaskManagerService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.taskManagerService.taskManagerDetail$
      .pipe(
        first(),
        flatMap(data => this.taskManagerService.loadLogList(data.id))
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

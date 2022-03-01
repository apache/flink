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
import { ActivatedRoute } from '@angular/router';
import { Subject } from 'rxjs';
import { mergeMap, takeUntil } from 'rxjs/operators';

import { StatusService, TaskManagerService } from 'services';

@Component({
  selector: 'flink-task-manager',
  templateUrl: './task-manager.component.html',
  styleUrls: ['./task-manager.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerComponent implements OnInit, OnDestroy {
  public isLoading = true;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly cdr: ChangeDetectorRef,
    private readonly activatedRoute: ActivatedRoute,
    private readonly taskManagerService: TaskManagerService,
    private readonly statusService: StatusService
  ) {}

  public ngOnInit(): void {
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        mergeMap(() => this.taskManagerService.loadManager(this.activatedRoute.snapshot.params.taskManagerId))
      )
      .subscribe(
        data => {
          this.taskManagerService.taskManagerDetail$.next(data);
          this.isLoading = false;
          this.cdr.markForCheck();
        },
        () => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }
      );
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

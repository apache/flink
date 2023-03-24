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

import { DatePipe, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { of, Subject } from 'rxjs';
import { catchError, mergeMap, takeUntil } from 'rxjs/operators';

import { BlockedBadgeComponent } from '@flink-runtime-web/components/blocked-badge/blocked-badge.component';
import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { NavigationComponent } from '@flink-runtime-web/components/navigation/navigation.component';
import { TaskManagerDetail } from '@flink-runtime-web/interfaces';
import { StatusService, TaskManagerService } from '@flink-runtime-web/services';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';

@Component({
  selector: 'flink-task-manager-status',
  templateUrl: './task-manager-status.component.html',
  styleUrls: ['./task-manager-status.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NgIf,
    BlockedBadgeComponent,
    NzDescriptionsModule,
    DatePipe,
    HumanizeBytesPipe,
    NavigationComponent,
    NzSkeletonModule
  ],
  standalone: true
})
export class TaskManagerStatusComponent implements OnInit, OnDestroy {
  public readonly listOfNavigation = [
    { path: 'metrics', title: 'Metrics' },
    { path: 'logs', title: 'Logs' },
    { path: 'stdout', title: 'Stdout' },
    { path: 'log-list', title: 'Log List' },
    { path: 'thread-dump', title: 'Thread Dump' }
  ];
  public taskManagerDetail?: TaskManagerDetail;
  public loading = true;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly statusService: StatusService,
    private readonly activatedRoute: ActivatedRoute,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.statusService.refresh$
      .pipe(
        mergeMap(() =>
          this.taskManagerService
            .loadManager(this.activatedRoute.snapshot.params.taskManagerId)
            .pipe(catchError(() => of(undefined)))
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.taskManagerDetail = data;
        this.loading = false;
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

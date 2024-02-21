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

import { DecimalPipe, NgForOf, NgIf, NgTemplateOutlet } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { of, Subject } from 'rxjs';
import { catchError, mergeMap, startWith, takeUntil } from 'rxjs/operators';

import { HumanizeBytesPipe } from '@flink-runtime-web/components/humanize-bytes.pipe';
import { MetricMap, TaskManagerDetail } from '@flink-runtime-web/interfaces';
import { StatusService, TaskManagerService } from '@flink-runtime-web/services';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

@Component({
  selector: 'flink-task-manager-metrics',
  templateUrl: './task-manager-metrics.component.html',
  styleUrls: ['./task-manager-metrics.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzCardModule,
    NzTableModule,
    NzToolTipModule,
    NzProgressModule,
    DecimalPipe,
    HumanizeBytesPipe,
    NzIconModule,
    NzGridModule,
    NgForOf,
    NgIf,
    NgTemplateOutlet
  ],
  standalone: true
})
export class TaskManagerMetricsComponent implements OnInit, OnDestroy {
  public taskManagerDetail?: TaskManagerDetail;
  public metrics: { [id: string]: number } = {};

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly activatedRoute: ActivatedRoute,
    private readonly statusService: StatusService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    const taskManagerId = this.activatedRoute.parent!.snapshot.params.taskManagerId;
    this.statusService.refresh$
      .pipe(
        startWith(true),
        mergeMap(() => this.taskManagerService.loadManager(taskManagerId).pipe(catchError(() => of(undefined)))),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        if (data) {
          this.reload(data.id);
        }
        this.taskManagerDetail = data;
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private reload(id: string): void {
    this.taskManagerService
      .loadMetrics(id, [
        'Status.JVM.Memory.Heap.Used',
        'Status.JVM.Memory.Heap.Max',
        'Status.Shuffle.Netty.UsedMemory',
        'Status.Shuffle.Netty.TotalMemory',
        'Status.Flink.Memory.Managed.Used',
        'Status.Flink.Memory.Managed.Total',
        'Status.JVM.Memory.Metaspace.Used',
        'Status.JVM.Memory.Metaspace.Max'
      ])
      .pipe(
        catchError(() => of({} as MetricMap)),
        takeUntil(this.destroy$)
      )
      .subscribe(metrics => {
        this.metrics = metrics;
        this.cdr.markForCheck();
      });
  }
}

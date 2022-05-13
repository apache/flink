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
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import { TaskManagerDetail } from '@flink-runtime-web/interfaces';
import { TaskManagerService } from '@flink-runtime-web/services';

import { TaskManagerLocalService } from '../task-manager-local.service';

@Component({
  selector: 'flink-task-manager-metrics',
  templateUrl: './task-manager-metrics.component.html',
  styleUrls: ['./task-manager-metrics.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerMetricsComponent implements OnInit, OnDestroy {
  public taskManagerDetail: TaskManagerDetail;
  public metrics: { [id: string]: number } = {};

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly taskManagerLocalService: TaskManagerLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.taskManagerLocalService
      .taskManagerDetailChanges()
      .pipe(takeUntil(this.destroy$))
      .subscribe(data => {
        this.taskManagerDetail = data;
        this.reload(data.id);
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
      .pipe(takeUntil(this.destroy$))
      .subscribe(metrics => {
        this.metrics = metrics;
        this.cdr.markForCheck();
      });
  }
}

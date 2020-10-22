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
import { TaskManagerDetailInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { TaskManagerService } from 'services';

@Component({
  selector: 'flink-task-manager-metrics',
  templateUrl: './task-manager-metrics.component.html',
  styleUrls: ['./task-manager-metrics.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerMetricsComponent implements OnInit, OnDestroy {
  private destroy$ = new Subject();
  taskManagerDetail: TaskManagerDetailInterface;
  metrics: { [id: string]: number } = {};

  constructor(private taskManagerService: TaskManagerService, private cdr: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.taskManagerService.taskManagerDetail$.pipe(takeUntil(this.destroy$)).subscribe(data => {
      this.taskManagerDetail = data;
      this.taskManagerService
        .getMetrics(data.id, [
          'Status.JVM.Memory.Heap.Used',
          'Status.JVM.Memory.Heap.Max',
          'Status.Shuffle.Netty.UsedMemory',
          'Status.Shuffle.Netty.TotalMemory',
          'Status.Flink.Memory.Managed.Used',
          'Status.Flink.Memory.Managed.Total',
          'Status.JVM.Memory.Metaspace.Used',
          'Status.JVM.Memory.Metaspace.Max'
        ])
        .subscribe(metrics => {
          this.metrics = metrics;
          this.cdr.markForCheck();
        });
      this.cdr.markForCheck();
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

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
import { map, takeUntil } from 'rxjs/operators';

import { JobManagerService, StatusService } from 'services';

@Component({
  selector: 'flink-job-manager-metrics',
  templateUrl: './job-manager-metrics.component.html',
  styleUrls: ['./job-manager-metrics.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobManagerMetricsComponent implements OnInit, OnDestroy {
  public metrics: { [id: string]: number } = {};
  public config: { [id: string]: string } = {};
  public listOfGCName: string[] = [];
  public listOfGCMetric: Array<{ name: string; count: number | null; time: number | null }> = [];

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobManagerService: JobManagerService,
    private readonly statusService: StatusService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.jobManagerService.loadConfig().subscribe(data => {
      for (const item of data) {
        this.config[item.key] = item.value;
      }
      this.cdr.markForCheck();
    });
    this.statusService.refresh$.pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.jobManagerService
        .getMetricsName()
        .pipe(map(arr => arr.filter(item => item.indexOf('Status.JVM.GarbageCollector') !== -1)))
        .subscribe(data => {
          this.listOfGCName = data;
          this.cdr.markForCheck();
        });
      this.jobManagerService
        .getMetrics([
          'Status.JVM.Memory.Heap.Used',
          'Status.JVM.Memory.Heap.Max',
          'Status.JVM.Memory.Metaspace.Used',
          'Status.JVM.Memory.Metaspace.Max',
          'Status.JVM.Memory.Heap.Committed',
          'Status.JVM.Memory.Heap.Used',
          'Status.JVM.Memory.Heap.Max',
          'Status.JVM.Memory.NonHeap.Committed',
          'Status.JVM.Memory.NonHeap.Used',
          'Status.JVM.Memory.NonHeap.Max',
          'Status.JVM.Memory.Direct.Count',
          'Status.JVM.Memory.Direct.MemoryUsed',
          'Status.JVM.Memory.Direct.TotalCapacity',
          'Status.JVM.Memory.Mapped.Count',
          'Status.JVM.Memory.Mapped.MemoryUsed',
          'Status.JVM.Memory.Mapped.TotalCapacity',
          ...this.listOfGCName
        ])
        .subscribe(data => {
          this.metrics = data;
          this.listOfGCMetric = Array.from(
            new Set(
              this.listOfGCName.map(item =>
                item
                  .replace('Status.JVM.GarbageCollector.', '')
                  .replace('.Count', '')
                  .replace('.Time', '')
              )
            )
          ).map(name => {
            return {
              name,
              count: this.metrics[`Status.JVM.GarbageCollector.${name}.Count`],
              time: this.metrics[`Status.JVM.GarbageCollector.${name}.Time`]
            };
          });
          this.cdr.markForCheck();
        });
    });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

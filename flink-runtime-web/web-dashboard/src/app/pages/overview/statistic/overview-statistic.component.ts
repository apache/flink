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
import { OverviewInterface, TaskmanagersItemCalInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';
import { JobManagerService, OverviewService, StatusService, TaskManagerService } from 'services';

@Component({
  selector: 'flink-overview-statistic',
  templateUrl: './overview-statistic.component.html',
  styleUrls: ['./overview-statistic.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewStatisticComponent implements OnInit, OnDestroy {
  statistic: OverviewInterface | null;
  listOfTaskManager: TaskmanagersItemCalInterface[] = [];
  taskSlotPercentage: number;
  taskManagerCPUs: number;
  totalCount: number;
  destroy$ = new Subject();
  timeoutThresholdSeconds: number;

  constructor(
    private statusService: StatusService,
    private overviewService: OverviewService,
    private taskManagerService: TaskManagerService,
    private cdr: ChangeDetectorRef,
    private jobManagerService: JobManagerService
  ) {}

  ngOnInit() {
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(() => this.overviewService.loadOverview())
      )
      .subscribe(data => {
        this.statistic = data;
        this.taskSlotPercentage = Math.round(
          ((data['slots-total'] - data['slots-available']) / data['slots-total']) * 100
        );
        this.cdr.markForCheck();
      });
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(() => this.taskManagerService.loadManagers())
      )
      .subscribe(data => {
        this.listOfTaskManager = data;
        this.totalCount = data.length;
        this.taskManagerCPUs = 0;
        this.listOfTaskManager.map(tm => {
          this.taskManagerCPUs += tm.hardware.cpuCores;
          tm.milliSecondsSinceLastHeartBeat = new Date().getTime() - tm.timeSinceLastHeartbeat;
        });

        this.cdr.markForCheck();
      });
    this.jobManagerService.loadConfig().subscribe(data => {
      const listOfConfig = data;

      let timeoutThreshold = '';
      listOfConfig.map(config => {
        if (config.key === 'akka.lookup.timeout') {
          timeoutThreshold = config.value;
        }
      });
      if (!timeoutThreshold) {
        this.timeoutThresholdSeconds = 20;
      } else {
        const intValue = parseInt(timeoutThreshold.substr(0, timeoutThreshold.length - 1), 10);
        if (timeoutThreshold.substr(-1, 1) === 's') {
          this.timeoutThresholdSeconds = intValue;
        } else if (timeoutThreshold.substr(-1, 1) === 'm') {
          this.timeoutThresholdSeconds = intValue * 60;
        } else if (timeoutThreshold.substr(-1, 1) === 'h') {
          this.timeoutThresholdSeconds = intValue * 60 * 60;
        } else {
          this.timeoutThresholdSeconds = 20;
        }
      }

      this.cdr.markForCheck();
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  trackByTaskManager(_: number, item: TaskmanagersItemCalInterface) {
    return item.id;
  }
}

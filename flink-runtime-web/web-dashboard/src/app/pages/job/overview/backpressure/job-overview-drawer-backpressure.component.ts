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

import { Component, OnInit, ChangeDetectionStrategy, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { Subject } from 'rxjs';
import { flatMap, takeUntil, tap } from 'rxjs/operators';
import { JobBackpressureInterface, JobBackpressureSubtaskInterface, NodesItemCorrectInterface } from 'interfaces';
import { JobService } from 'services';

@Component({
  selector: 'flink-job-overview-drawer-backpressure',
  templateUrl: './job-overview-drawer-backpressure.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls: ['./job-overview-drawer-backpressure.component.less']
})
export class JobOverviewDrawerBackpressureComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  isLoading = true;
  now = Date.now();
  selectedVertex: NodesItemCorrectInterface | null;
  backpressure = {} as JobBackpressureInterface;
  listOfSubTaskBackpressure: JobBackpressureSubtaskInterface[] = [];

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}
  trackBackPressureBy(_: number, node: JobBackpressureSubtaskInterface) {
    return node.subtask;
  }

  ngOnInit() {
    this.jobService.jobWithVertex$
      .pipe(
        takeUntil(this.destroy$),
        tap(data => (this.selectedVertex = data.vertex)),
        flatMap(data => this.jobService.loadOperatorBackPressure(data.job.jid, data.vertex!.id))
      )
      .subscribe(
        data => {
          this.isLoading = false;
          this.now = Date.now();
          this.backpressure = data;
          this.listOfSubTaskBackpressure = data.subtasks || [];
          this.cdr.markForCheck();
        },
        () => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }
      );
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

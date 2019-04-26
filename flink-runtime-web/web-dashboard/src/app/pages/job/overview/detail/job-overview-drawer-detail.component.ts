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

import { Component, ChangeDetectionStrategy, OnInit, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { NodesItemCorrectInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { JobService } from 'services';

@Component({
  selector: 'flink-job-overview-drawer-detail',
  templateUrl: './job-overview-drawer-detail.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls: ['./job-overview-drawer-detail.component.less']
})
export class JobOverviewDrawerDetailComponent implements OnInit, OnDestroy {
  destroy$ = new Subject();
  node: NodesItemCorrectInterface | null;

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.jobService.selectedVertex$.pipe(takeUntil(this.destroy$)).subscribe(node => {
      this.node = node;
      this.cdr.markForCheck();
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}

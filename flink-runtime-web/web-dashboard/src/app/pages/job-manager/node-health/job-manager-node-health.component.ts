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

import { DatePipe, NgForOf, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { of, Subject } from 'rxjs';
import { catchError, takeUntil } from 'rxjs/operators';

import { BlockedNodeInfo } from '@flink-runtime-web/interfaces';
import { JobManagerService } from '@flink-runtime-web/services';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzTooltipModule } from 'ng-zorro-antd/tooltip';

@Component({
  selector: 'flink-job-manager-node-health',
  templateUrl: './job-manager-node-health.component.html',
  styleUrls: ['./job-manager-node-health.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzTableModule, NzTagModule, NzSpinModule, NzEmptyModule, NzTooltipModule, DatePipe, NgForOf, NgIf]
})
export class JobManagerNodeHealthComponent implements OnInit, OnDestroy {
  blockedNodes: BlockedNodeInfo[] = [];
  loading = true;
  private destroy$ = new Subject<void>();

  readonly trackByNodeId = (_: number, item: BlockedNodeInfo): string => item.nodeId;

  constructor(
    private readonly jobManagerService: JobManagerService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.jobManagerService
      .loadBlocklist()
      .pipe(
        catchError(() => of({ blockedNodes: [] })),
        takeUntil(this.destroy$)
      )
      .subscribe(response => {
        this.loading = false;
        this.blockedNodes = response.blockedNodes || [];
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  isExpired(endTimestamp: number): boolean {
    return endTimestamp > 0 && endTimestamp < Date.now();
  }

  formatExpiration(endTimestamp: number): string {
    if (endTimestamp <= 0) {
      return 'Never';
    }
    return new Date(endTimestamp).toLocaleString();
  }
}

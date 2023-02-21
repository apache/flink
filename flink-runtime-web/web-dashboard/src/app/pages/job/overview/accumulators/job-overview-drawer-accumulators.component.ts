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

import { DecimalPipe, NgIf } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { of, Subject } from 'rxjs';
import { catchError, mergeMap, takeUntil } from 'rxjs/operators';

import { JobAccumulators, SubTaskAccumulators, UserAccumulators } from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { typeDefinition } from '@flink-runtime-web/utils/strong-type';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';

import { JobLocalService } from '../../job-local.service';

@Component({
  selector: 'flink-job-overview-drawer-accumulators',
  templateUrl: './job-overview-drawer-accumulators.component.html',
  styleUrls: ['./job-overview-drawer-accumulators.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzTabsModule, NzTableModule, NgIf, DecimalPipe],
  standalone: true
})
export class JobOverviewDrawerAccumulatorsComponent implements OnInit, OnDestroy {
  public readonly trackByName = (_: number, node: UserAccumulators): string => node.name;

  public listOfAccumulator: UserAccumulators[] = [];
  public listOfSubTaskAccumulator: SubTaskAccumulators[] = [];
  public isLoading = true;
  public virtualItemSize = 36;
  public readonly narrowUserAccumulators = typeDefinition<UserAccumulators>();
  public readonly narrowSubTaskAccumulators = typeDefinition<SubTaskAccumulators>();

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.jobLocalService
      .jobWithVertexChanges()
      .pipe(
        mergeMap(data =>
          this.jobService.loadAccumulators(data.job.jid, data.vertex!.id).pipe(
            catchError(() => {
              return of({
                main: [],
                subtasks: []
              } as JobAccumulators);
            })
          )
        ),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.isLoading = false;
        this.listOfAccumulator = data.main;
        this.listOfSubTaskAccumulator = this.transformToSubTaskAccumulator(data.subtasks) || [];
        this.cdr.markForCheck();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private transformToSubTaskAccumulator(list: SubTaskAccumulators[]): SubTaskAccumulators[] {
    const transformed: SubTaskAccumulators[] = [];
    list.forEach(accumulator => {
      // @ts-ignore
      accumulator['user-accumulators'].forEach(userAccumulator => {
        transformed.push({
          ...accumulator,
          name: userAccumulator.name,
          type: userAccumulator.type,
          value: userAccumulator.value
        });
      });
    });
    return transformed;
  }
}

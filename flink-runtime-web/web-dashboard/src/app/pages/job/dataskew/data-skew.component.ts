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

import { NgForOf } from '@angular/common';
import { ChangeDetectionStrategy, Component, OnDestroy, OnInit, ChangeDetectorRef } from '@angular/core';
import { Subject } from 'rxjs';
import { distinctUntilChanged, takeUntil, map, mergeMap } from 'rxjs/operators';

import { JobDetailCorrect } from '@flink-runtime-web/interfaces';
import { MetricsService } from '@flink-runtime-web/services';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTableModule } from 'ng-zorro-antd/table';

import { JobLocalService } from '../job-local.service';

@Component({
  selector: 'flink-data-skew',
  templateUrl: './data-skew.component.html',
  styleUrls: ['./data-skew.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzButtonModule, NzCardModule, NzTableModule, NgForOf, NzIconModule],
  standalone: true
})
export class DataSkewComponent implements OnInit, OnDestroy {
  public listOfVerticesAndSkew: Array<{ vertexName: string; skewPct: number }> = [];
  public isLoading = true;
  public jobDetail: JobDetailCorrect;

  private destroy$ = new Subject<void>();
  private refresh$ = new Subject<void>();

  constructor(
    private readonly metricsService: MetricsService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.refresh$
      .pipe(
        map(() => {
          return this.jobDetail;
        }),
        takeUntil(this.destroy$)
      )
      .pipe(map(jobDetail => jobDetail.vertices))
      .pipe(
        mergeMap(vertices => {
          const result: Array<{ vertexName: string; skewPct: number }> = [];
          vertices.forEach(v => {
            this.metricsService
              .loadAggregatedMetrics(this.jobDetail.jid, v.id, ['numRecordsIn'], 'skew')
              .subscribe(metricMap => {
                const skew = Number.isNaN(+metricMap['numRecordsIn']) ? 0 : Math.round(metricMap['numRecordsIn']);
                result.push({ vertexName: v.name, skewPct: skew });
                result.sort((a, b) => (a.skewPct > b.skewPct ? -1 : 1));
                this.isLoading = false;
                this.listOfVerticesAndSkew = result;
                this.cdr.markForCheck();
              });
          });
          return result;
        })
      )
      .subscribe(_ => {}); // no-op subscriber to trigger the execution of the lazy processing

    this.jobLocalService
      .jobDetailChanges()
      .pipe(
        distinctUntilChanged((pre, next) => pre.jid === next.jid),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.jobDetail = data;
        this.cdr.markForCheck();
        this.refresh$.next();
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.refresh$.complete();
  }

  public refresh(): void {
    this.refresh$.next();
  }
}

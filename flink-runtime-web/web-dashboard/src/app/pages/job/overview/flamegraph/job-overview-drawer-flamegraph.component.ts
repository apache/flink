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

import { NgIf, NgSwitch, NgSwitchCase, NgSwitchDefault } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Subject } from 'rxjs';
import { mergeMap, takeUntil, tap } from 'rxjs/operators';

import { FlameGraphComponent } from '@flink-runtime-web/components/flame-graph/flame-graph.component';
import { HumanizeDurationPipe } from '@flink-runtime-web/components/humanize-duration.pipe';
import { JobFlameGraph, NodesItemCorrect } from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { NzRadioModule } from 'ng-zorro-antd/radio';
import { NzSpinModule } from 'ng-zorro-antd/spin';

import { JobLocalService } from '../../job-local.service';

@Component({
  selector: 'flink-job-overview-drawer-flamegraph',
  templateUrl: './job-overview-drawer-flamegraph.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls: ['./job-overview-drawer-flamegraph.component.less'],
  imports: [
    NgIf,
    NzRadioModule,
    FormsModule,
    NgSwitch,
    HumanizeDurationPipe,
    FlameGraphComponent,
    NgSwitchCase,
    NgSwitchDefault,
    NzSpinModule
  ],
  standalone: true
})
export class JobOverviewDrawerFlameGraphComponent implements OnInit, OnDestroy {
  public isLoading = true;
  public now = Date.now();
  public selectedVertex: NodesItemCorrect | null;
  public flameGraph = {} as JobFlameGraph;

  public graphType = 'on_cpu';

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.requestFlameGraph();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private requestFlameGraph(): void {
    this.jobLocalService
      .jobWithVertexChanges()
      .pipe(
        tap(data => (this.selectedVertex = data.vertex)),
        mergeMap(data => this.jobService.loadOperatorFlameGraph(data.job.jid, data.vertex!.id, this.graphType)),
        takeUntil(this.destroy$)
      )
      .subscribe(
        data => {
          this.now = Date.now();
          if (this.flameGraph.endTimestamp !== data['endTimestamp']) {
            this.isLoading = false;
            this.flameGraph = data;
            this.flameGraph.graphType = this.graphType;
          }
          this.cdr.markForCheck();
        },
        () => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }
      );
  }

  public selectFrameGraphType(): void {
    this.destroy$.next();
    this.flameGraph = {} as JobFlameGraph;
    this.requestFlameGraph();
  }
}

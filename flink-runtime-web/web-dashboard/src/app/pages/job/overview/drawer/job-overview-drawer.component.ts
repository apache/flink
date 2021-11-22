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

import { animate, style, transition, trigger } from '@angular/animations';
import { ChangeDetectionStrategy, Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { combineLatest, Subject } from 'rxjs';
import { map, takeUntil } from 'rxjs/operators';

import { JobChartService } from 'share/customize/job-chart/job-chart.service';

import { JobService } from 'services';

@Component({
  selector: 'flink-job-overview-drawer',
  templateUrl: './job-overview-drawer.component.html',
  styleUrls: ['./job-overview-drawer.component.less'],
  animations: [
    trigger('drawer', [
      transition('void => *', [
        style({ transform: 'translateX(100%)' }),
        animate(100, style({ transform: 'translateX(0)' }))
      ]),
      transition('* => void', [
        style({ transform: 'translateX(0)' }),
        animate(100, style({ transform: 'translateX(100%)' }))
      ])
    ])
  ],
  host: {
    '[@drawer]': '',
    '[class.full-width]': 'fullScreen'
  },
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobOverviewDrawerComponent implements OnInit, OnDestroy {
  public readonly listOfNavigation = [
    { title: 'Detail', path: 'detail' },
    { title: 'SubTasks', path: 'subtasks' },
    { title: 'TaskManagers', path: 'taskmanagers' },
    { title: 'Watermarks', path: 'watermarks' },
    { title: 'Accumulators', path: 'accumulators' },
    { title: 'BackPressure', path: 'backpressure' },
    { title: 'Metrics', path: 'metrics' },
    { title: 'FlameGraph', path: 'flamegraph' }
  ];

  public fullScreen = false;

  private cachePath = this.listOfNavigation[0].path;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly activatedRoute: ActivatedRoute,
    private readonly router: Router,
    private readonly jobService: JobService,
    private readonly jobChartService: JobChartService
  ) {}

  public ngOnInit(): void {
    const nodeId$ = this.activatedRoute.params.pipe(map(item => item.vertexId));
    combineLatest([this.jobService.jobDetail$.pipe(map(item => item.plan.nodes)), nodeId$])
      .pipe(takeUntil(this.destroy$))
      .subscribe(([nodes, nodeId]) => {
        if (!this.activatedRoute.firstChild) {
          this.router.navigate([this.cachePath], { relativeTo: this.activatedRoute }).then();
        } else {
          this.cachePath = this.activatedRoute.firstChild.snapshot.data.path;
        }
        if (nodes && nodeId) {
          this.jobService.selectedVertex$.next(nodes.find(item => item.id === nodeId));
        }
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.jobService.selectedVertex$.next(null);
  }

  public closeDrawer(): void {
    if (this.fullScreen) {
      this.fullScreen = false;
      this.jobChartService.resize();
    } else {
      this.router.navigate(['../../'], { relativeTo: this.activatedRoute }).then();
    }
  }

  public fullDrawer(): void {
    this.fullScreen = true;
    this.jobChartService.resize();
  }
}

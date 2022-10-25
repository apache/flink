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
import { ChangeDetectionStrategy, Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router, RouterOutlet } from '@angular/router';
import { combineLatest, Subject } from 'rxjs';
import { map, takeUntil } from 'rxjs/operators';

import { JobChartService } from '@flink-runtime-web/components/job-chart/job-chart.service';
import { NavigationComponent } from '@flink-runtime-web/components/navigation/navigation.component';
import { RouterTab } from '@flink-runtime-web/core/module-config';
import {
  JOB_OVERVIEW_MODULE_CONFIG,
  JOB_OVERVIEW_MODULE_DEFAULT_CONFIG,
  JobOverviewModuleConfig
} from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { NzIconModule } from 'ng-zorro-antd/icon';

import { JobLocalService } from '../../job-local.service';

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
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NavigationComponent, RouterOutlet, NzIconModule],
  standalone: true
})
export class JobOverviewDrawerComponent implements OnInit, OnDestroy {
  public readonly listOfNavigation: RouterTab[] = [];

  public fullScreen = false;

  private cachePath: string;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly activatedRoute: ActivatedRoute,
    private readonly router: Router,
    private readonly jobLocalService: JobLocalService,
    private readonly jobChartService: JobChartService,
    @Inject(JOB_OVERVIEW_MODULE_CONFIG) readonly moduleConfig: JobOverviewModuleConfig
  ) {
    this.listOfNavigation = moduleConfig.routerTabs || JOB_OVERVIEW_MODULE_DEFAULT_CONFIG.routerTabs;
    this.cachePath = this.listOfNavigation[0]?.path;
  }

  public ngOnInit(): void {
    const nodeId$ = this.activatedRoute.params.pipe(map(item => item.vertexId));
    combineLatest([this.jobLocalService.jobDetailChanges().pipe(map(item => item.plan.nodes)), nodeId$])
      .pipe(takeUntil(this.destroy$))
      .subscribe(([nodes, nodeId]) => {
        if (!this.activatedRoute.firstChild) {
          this.router.navigate([this.cachePath], { relativeTo: this.activatedRoute }).then();
        } else {
          this.cachePath = this.activatedRoute.firstChild.snapshot.data.path;
        }
        if (nodes && nodeId) {
          this.jobLocalService.setSelectedVertex(nodes.find(item => item.id === nodeId) || null);
        }
      });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.jobLocalService.setSelectedVertex(null);
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

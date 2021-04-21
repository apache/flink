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

import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { JobOverviewDrawerAccumulatorsComponent } from './accumulators/job-overview-drawer-accumulators.component';
import { JobOverviewDrawerBackpressureComponent } from './backpressure/job-overview-drawer-backpressure.component';
import { JobOverviewDrawerFlameGraphComponent } from './flamegraph/job-overview-drawer-flamegraph.component';
import { JobOverviewDrawerChartComponent } from './chart/job-overview-drawer-chart.component';
import { JobOverviewDrawerDetailComponent } from './detail/job-overview-drawer-detail.component';
import { JobOverviewDrawerComponent } from './drawer/job-overview-drawer.component';
import { JobOverviewComponent } from './job-overview.component';
import { JobOverviewDrawerSubtasksComponent } from './subtasks/job-overview-drawer-subtasks.component';
import { JobOverviewDrawerTaskmanagersComponent } from './taskmanagers/job-overview-drawer-taskmanagers.component';
import { JobOverviewDrawerWatermarksComponent } from './watermarks/job-overview-drawer-watermarks.component';

const routes: Routes = [
  {
    path: '',
    component: JobOverviewComponent,
    children: [
      {
        path: ':vertexId',
        component: JobOverviewDrawerComponent,
        children: [
          {
            path: 'detail',
            component: JobOverviewDrawerDetailComponent,
            data: {
              path: 'detail'
            }
          },
          {
            path: 'subtasks',
            component: JobOverviewDrawerSubtasksComponent,
            data: {
              path: 'subtasks'
            }
          },
          {
            path: 'taskmanagers',
            component: JobOverviewDrawerTaskmanagersComponent,
            data: {
              path: 'taskmanagers'
            }
          },
          {
            path: 'watermarks',
            component: JobOverviewDrawerWatermarksComponent,
            data: {
              path: 'watermarks'
            }
          },
          {
            path: 'accumulators',
            component: JobOverviewDrawerAccumulatorsComponent,
            data: {
              path: 'accumulators'
            }
          },
          {
            path: 'metrics',
            component: JobOverviewDrawerChartComponent,
            data: {
              path: 'metrics'
            }
          },
          {
            path: 'backpressure',
            component: JobOverviewDrawerBackpressureComponent,
            data: {
              path: 'backpressure'
            }
          },
          {
            path: 'flamegraph',
            component: JobOverviewDrawerFlameGraphComponent,
            data: {
              path: 'flamegraph'
            }
          }
        ]
      }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class JobOverviewRoutingModule {}

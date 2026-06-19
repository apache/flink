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

import { Routes } from '@angular/router';

import { JobOverviewDrawerComponent } from './drawer/job-overview-drawer.component';
import { JobOverviewComponent } from './job-overview.component';

export const JOB_OVERVIEW_ROUTES: Routes = [
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
            loadComponent: () =>
              import('./detail/job-overview-drawer-detail.component').then(m => m.JobOverviewDrawerDetailComponent),
            data: {
              path: 'detail'
            }
          },
          {
            path: 'subtasks',
            loadComponent: () =>
              import('./subtasks/job-overview-drawer-subtasks.component').then(
                m => m.JobOverviewDrawerSubtasksComponent
              ),
            data: {
              path: 'subtasks'
            }
          },
          {
            path: 'taskmanagers',
            loadComponent: () =>
              import('./taskmanagers/job-overview-drawer-taskmanagers.component').then(
                m => m.JobOverviewDrawerTaskmanagersComponent
              ),
            data: {
              path: 'taskmanagers'
            }
          },
          {
            path: 'watermarks',
            loadComponent: () =>
              import('./watermarks/job-overview-drawer-watermarks.component').then(
                m => m.JobOverviewDrawerWatermarksComponent
              ),
            data: {
              path: 'watermarks'
            }
          },
          {
            path: 'accumulators',
            loadComponent: () =>
              import('./accumulators/job-overview-drawer-accumulators.component').then(
                m => m.JobOverviewDrawerAccumulatorsComponent
              ),
            data: {
              path: 'accumulators'
            }
          },
          {
            path: 'metrics',
            loadComponent: () =>
              import('./chart/job-overview-drawer-chart.component').then(m => m.JobOverviewDrawerChartComponent),
            data: {
              path: 'metrics'
            }
          },
          {
            path: 'backpressure',
            loadComponent: () =>
              import('./backpressure/job-overview-drawer-backpressure.component').then(
                m => m.JobOverviewDrawerBackpressureComponent
              ),
            data: {
              path: 'backpressure'
            }
          },
          {
            path: 'flamegraph',
            loadComponent: () =>
              import('./flamegraph/job-overview-drawer-flamegraph.component').then(
                m => m.JobOverviewDrawerFlameGraphComponent
              ),
            data: {
              path: 'flamegraph'
            }
          }
        ]
      }
    ]
  }
];

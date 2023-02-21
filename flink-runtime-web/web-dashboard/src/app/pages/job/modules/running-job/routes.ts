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

import { JobDetailComponent } from '@flink-runtime-web/pages/job/job-detail/job-detail.component';
import { RunningJobGuard } from '@flink-runtime-web/pages/job/modules/running-job/running-job.guard';

export const RUNNING_JOB_ROUTES: Routes = [
  {
    path: '',
    component: JobDetailComponent,
    canActivate: [RunningJobGuard],
    children: [
      {
        path: 'overview',
        loadChildren: () => import('../../overview/routes').then(m => m.JOB_OVERVIEW_ROUTES),
        data: {
          path: 'overview'
        }
      },
      {
        path: 'timeline',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/timeline/job-timeline.component').then(m => m.JobTimelineComponent),
        data: {
          path: 'timeline'
        }
      },
      {
        path: 'exceptions',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/exceptions/job-exceptions.component').then(
            m => m.JobExceptionsComponent
          ),
        data: {
          path: 'exceptions'
        }
      },
      {
        path: 'checkpoints',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/checkpoints/job-checkpoints.component').then(
            m => m.JobCheckpointsComponent
          ),
        data: {
          path: 'checkpoints'
        }
      },
      {
        path: 'configuration',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/configuration/job-configuration.component').then(
            m => m.JobConfigurationComponent
          ),
        data: {
          path: 'configuration'
        }
      },
      { path: '**', redirectTo: 'overview', pathMatch: 'full' }
    ]
  }
];

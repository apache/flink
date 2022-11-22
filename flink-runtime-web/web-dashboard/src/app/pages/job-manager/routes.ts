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

import { JobManagerComponent } from '@flink-runtime-web/pages/job-manager/job-manager.component';

export const JOB_MANAGER_ROUTES: Routes = [
  {
    path: '',
    component: JobManagerComponent,
    children: [
      {
        path: 'metrics',
        loadComponent: () => import('./metrics/job-manager-metrics.component').then(m => m.JobManagerMetricsComponent),
        data: {
          path: 'metrics'
        }
      },
      {
        path: 'config',
        loadComponent: () =>
          import('./configuration/job-manager-configuration.component').then(m => m.JobManagerConfigurationComponent),
        data: {
          path: 'config'
        }
      },
      {
        path: 'logs',
        loadComponent: () => import('./logs/job-manager-logs.component').then(m => m.JobManagerLogsComponent),
        data: {
          path: 'logs'
        }
      },
      {
        path: 'stdout',
        loadComponent: () => import('./stdout/job-manager-stdout.component').then(m => m.JobManagerStdoutComponent),
        data: {
          path: 'stdout'
        }
      },
      {
        path: 'log',
        loadComponent: () =>
          import('./log-list/job-manager-log-list.component').then(m => m.JobManagerLogListComponent),
        data: {
          path: 'log'
        }
      },
      {
        path: 'log/:logName',
        loadComponent: () =>
          import('./log-detail/job-manager-log-detail.component').then(m => m.JobManagerLogDetailComponent),
        data: {
          path: 'log'
        }
      },
      {
        path: 'thread-dump',
        loadComponent: () =>
          import('./thread-dump/job-manager-thread-dump.component').then(m => m.JobManagerThreadDumpComponent),
        data: {
          path: 'thread-dump'
        }
      },
      {
        path: '**',
        redirectTo: 'metrics',
        pathMatch: 'full'
      }
    ]
  }
];

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

import { TaskManagerListComponent } from './list/task-manager-list.component';
import { TaskManagerComponent } from './task-manager.component';

export const TASK_MANAGER_ROUTES: Routes = [
  {
    path: '',
    component: TaskManagerListComponent
  },
  {
    path: ':taskManagerId',
    component: TaskManagerComponent,
    children: [
      {
        path: 'metrics',
        loadComponent: () =>
          import('./metrics/task-manager-metrics.component').then(m => m.TaskManagerMetricsComponent),
        data: {
          path: 'metrics'
        }
      },
      {
        path: 'log-list',
        loadComponent: () =>
          import('./log-list/task-manager-log-list.component').then(m => m.TaskManagerLogListComponent),
        data: {
          path: 'log-list'
        }
      },
      {
        path: 'thread-dump',
        loadComponent: () =>
          import('./thread-dump/task-manager-thread-dump.component').then(m => m.TaskManagerThreadDumpComponent),
        data: {
          path: 'thread-dump'
        }
      },
      {
        path: 'log-list/:logName',
        loadComponent: () =>
          import('./log-detail/task-manager-log-detail.component').then(m => m.TaskManagerLogDetailComponent),
        data: {
          path: 'log-list'
        }
      },
      {
        path: 'logs',
        loadComponent: () => import('./logs/task-manager-logs.component').then(m => m.TaskManagerLogsComponent),
        data: {
          path: 'logs'
        }
      },
      {
        path: 'stdout',
        loadComponent: () => import('./stdout/task-manager-stdout.component').then(m => m.TaskManagerStdoutComponent),
        data: {
          path: 'stdout'
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

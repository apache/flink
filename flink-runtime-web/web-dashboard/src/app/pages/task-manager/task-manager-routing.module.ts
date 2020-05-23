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
import { TaskManagerLogDetailComponent } from './log-detail/task-manager-log-detail.component';
import { TaskManagerLogListComponent } from './log-list/task-manager-log-list.component';
import { TaskManagerComponent } from './task-manager.component';
import { TaskManagerListComponent } from './list/task-manager-list.component';
import { TaskManagerLogsComponent } from './logs/task-manager-logs.component';
import { TaskManagerMetricsComponent } from './metrics/task-manager-metrics.component';
import { TaskManagerStdoutComponent } from './stdout/task-manager-stdout.component';
import { TaskManagerThreadDumpComponent } from './thread-dump/task-manager-thread-dump.component';

const routes: Routes = [
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
        component: TaskManagerMetricsComponent,
        data: {
          path: 'metrics'
        }
      },
      {
        path: 'log-list',
        component: TaskManagerLogListComponent,
        data: {
          path: 'log-list'
        }
      },
      {
        path: 'thread-dump',
        component: TaskManagerThreadDumpComponent,
        data: {
          path: 'thread-dump'
        }
      },
      {
        path: 'log-list/:logName',
        component: TaskManagerLogDetailComponent,
        data: {
          path: 'log-list'
        }
      },
      {
        path: 'logs',
        component: TaskManagerLogsComponent,
        data: {
          path: 'logs'
        }
      },
      {
        path: 'stdout',
        component: TaskManagerStdoutComponent,
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

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class TaskManagerRoutingModule {}

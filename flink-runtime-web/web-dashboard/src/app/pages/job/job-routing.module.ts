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
import { JobListComponent } from 'share/customize/job-list/job-list.component';
import { JobCheckpointsComponent } from './checkpoints/job-checkpoints.component';
import { JobConfigurationComponent } from './configuration/job-configuration.component';
import { JobExceptionsComponent } from './exceptions/job-exceptions.component';
import { JobComponent } from './job.component';
import { JobTimelineComponent } from './timeline/job-timeline.component';

const routes: Routes = [
  {
    path: 'running',
    component: JobListComponent,
    data: {
      title: 'Running Jobs',
      completed: false
    }
  },
  {
    path: 'completed',
    component: JobListComponent,
    data: {
      title: 'Completed Jobs',
      completed: true
    }
  },
  {
    path: ':jid',
    component: JobComponent,
    children: [
      {
        path: 'overview',
        loadChildren: './overview/job-overview.module#JobOverviewModule',
        data: {
          path: 'overview'
        }
      },
      {
        path: 'timeline',
        component: JobTimelineComponent,
        data: {
          path: 'timeline'
        }
      },
      {
        path: 'exceptions',
        component: JobExceptionsComponent,
        data: {
          path: 'exceptions'
        }
      },
      {
        path: 'checkpoints',
        component: JobCheckpointsComponent,
        data: {
          path: 'checkpoints'
        }
      },
      {
        path: 'configuration',
        component: JobConfigurationComponent,
        data: {
          path: 'configuration'
        }
      },
      { path: '**', redirectTo: 'overview', pathMatch: 'full' }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class JobRoutingModule {}

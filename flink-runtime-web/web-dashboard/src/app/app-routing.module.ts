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
import { PreloadAllModules, RouterModule, Routes } from '@angular/router';

const routes: Routes = [
  { path: 'overview', loadChildren: () => import('./pages/overview/overview.module').then(m => m.OverviewModule) },
  { path: 'submit', loadChildren: () => import('./pages/submit/submit.module').then(m => m.SubmitModule) },
  {
    path: 'job-manager',
    loadChildren: () => import('./pages/job-manager/job-manager.module').then(m => m.JobManagerModule)
  },
  {
    path: 'task-manager',
    loadChildren: () => import('./pages/task-manager/task-manager.module').then(m => m.TaskManagerModule)
  },
  { path: 'job', loadChildren: () => import('./pages/job/job.module').then(m => m.JobModule) },
  { path: '**', redirectTo: 'overview', pathMatch: 'full' }
];

@NgModule({
  exports: [RouterModule],
  imports: [
    RouterModule.forRoot(routes, {
      useHash: true,
      preloadingStrategy: PreloadAllModules,
      relativeLinkResolution: 'legacy'
    })
  ]
})
export class AppRoutingModule {}

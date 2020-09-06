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
  { path: 'overview', loadChildren: './pages/overview/overview.module#OverviewModule' },
  { path: 'submit', loadChildren: './pages/submit/submit.module#SubmitModule' },
  { path: 'job-manager', loadChildren: './pages/job-manager/job-manager.module#JobManagerModule' },
  { path: 'task-manager', loadChildren: './pages/task-manager/task-manager.module#TaskManagerModule' },
  { path: 'job', loadChildren: './pages/job/job.module#JobModule' },
  { path: '**', redirectTo: 'overview', pathMatch: 'full' }
];

@NgModule({
  exports: [RouterModule],
  imports: [RouterModule.forRoot(routes, { useHash: true, preloadingStrategy: PreloadAllModules })]
})
export class AppRoutingModule {}

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

export const APP_ROUTES: Routes = [
  {
    path: 'overview',
    loadComponent: () => import('./pages/overview/overview.component').then(m => m.OverviewComponent)
  },
  { path: 'submit', loadComponent: () => import('./pages/submit/submit.component').then(m => m.SubmitComponent) },
  {
    path: 'job-manager',
    loadChildren: () => import('./pages/job-manager/routes').then(m => m.JOB_MANAGER_ROUTES)
  },
  {
    path: 'task-manager',
    loadChildren: () => import('./pages/task-manager/routes').then(m => m.TASK_MANAGER_ROUTES)
  },
  { path: 'job', loadChildren: () => import('./pages/job/routes').then(m => m.JOB_ROUTES) },
  { path: '**', redirectTo: 'overview', pathMatch: 'full' }
];

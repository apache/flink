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

import { ApplicationDetailComponent } from '@flink-runtime-web/pages/application/application-detail/application-detail.component';
import { RunningApplicationGuard } from '@flink-runtime-web/pages/application/modules/running-application/running-application.guard';

export const RUNNING_APPLICATION_ROUTES: Routes = [
  {
    path: '',
    component: ApplicationDetailComponent,
    canActivate: [RunningApplicationGuard],
    children: [
      {
        path: 'overview',
        loadChildren: () => import('../../overview/routes').then(m => m.APPLICATION_OVERVIEW_ROUTES),
        data: {
          path: 'overview'
        }
      },
      { path: '**', redirectTo: 'overview', pathMatch: 'full' }
    ]
  }
];

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

import { ApplicationLocalService } from '@flink-runtime-web/pages/application/application-local.service';
import { ApplicationComponent } from '@flink-runtime-web/pages/application/application.component';

export const APPLICATION_ROUTES: Routes = [
  {
    path: '',
    providers: [ApplicationLocalService],
    children: [
      {
        path: 'running',
        component: ApplicationComponent,
        children: [
          {
            path: ':id',
            loadChildren: () => import('./modules/running-application/routes').then(m => m.RUNNING_APPLICATION_ROUTES)
          }
        ]
      },
      {
        path: 'completed',
        component: ApplicationComponent,
        children: [
          {
            path: ':id',
            loadChildren: () =>
              import('./modules/completed-application/routes').then(m => m.COMPLETED_APPLICATION_ROUES)
          }
        ]
      }
    ]
  }
];

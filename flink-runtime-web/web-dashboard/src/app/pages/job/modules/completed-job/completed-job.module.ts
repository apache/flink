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

import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { CompletedJobRoutingModule } from '@flink-runtime-web/pages/job/modules/completed-job/completed-job-routing.module';
import {
  JOB_OVERVIEW_MODULE_CONFIG,
  JobOverviewModuleConfig
} from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { ShareModule } from '@flink-runtime-web/share/share.module';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';

const OVERRIDE_JOB_OVERVIEW_MODULE_CONFIG_FACTORY = (): JobOverviewModuleConfig => {
  return {
    routerTabs: [
      { title: 'Detail', path: 'detail' },
      { title: 'SubTasks', path: 'subtasks' },
      { title: 'TaskManagers', path: 'taskmanagers' }
    ]
  };
};

@NgModule({
  declarations: [],
  imports: [CommonModule, CompletedJobRoutingModule, ShareModule, NzIconModule, NzSkeletonModule, NzAlertModule],
  providers: [
    {
      provide: JOB_OVERVIEW_MODULE_CONFIG,
      useFactory: OVERRIDE_JOB_OVERVIEW_MODULE_CONFIG_FACTORY
    }
  ]
})
export class CompletedJobModule {}

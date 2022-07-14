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

import { JOB_MODULE_CONFIG, JOB_MODULE_DEFAULT_CONFIG, JobModuleConfig } from '@flink-runtime-web/pages/job/job.config';
import { CompletedJobRoutingModule } from '@flink-runtime-web/pages/job/modules/completed-job/completed-job-routing.module';
import {
  JOB_OVERVIEW_MODULE_CONFIG,
  JOB_OVERVIEW_MODULE_DEFAULT_CONFIG,
  JobOverviewModuleConfig
} from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { StatusService } from '@flink-runtime-web/services';
import { ShareModule } from '@flink-runtime-web/share/share.module';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzModalModule } from 'ng-zorro-antd/modal';
import { NzPipesModule } from 'ng-zorro-antd/pipes';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';

import { ClusterConfigComponent } from './cluster-config/cluster-config.component';
import { CompletedJobSubtasksTableActionComponent } from './subtasks-table-action/completed-job-subtasks-table-action.component';
import { CompletedJobTaskmanagersTableActionComponent } from './taskmanagers-table-action/completed-job-taskmanagers-table-action.component';

const OVERRIDE_JOB_OVERVIEW_MODULE_CONFIG_FACTORY = (statusService: StatusService): JobOverviewModuleConfig => {
  const isHistoryServer = statusService.configuration.features['web-history'];
  return {
    customComponents: isHistoryServer
      ? {
          ...JOB_OVERVIEW_MODULE_DEFAULT_CONFIG.customComponents,
          subtaskActionComponent: CompletedJobSubtasksTableActionComponent,
          taskManagerActionComponent: CompletedJobTaskmanagersTableActionComponent
        }
      : JOB_OVERVIEW_MODULE_DEFAULT_CONFIG.customComponents
  };
};

const OVERRIDE_JOB_MODULE_CONFIG_FACTORY = (statusService: StatusService): JobModuleConfig => {
  const isHistoryServer = statusService.configuration.features['web-history'];
  return {
    routerTabs: isHistoryServer
      ? [
          { title: 'Overview', path: 'overview' },
          { title: 'Exceptions', path: 'exceptions' },
          { title: 'TimeLine', path: 'timeline' },
          { title: 'Checkpoints', path: 'checkpoints' },
          { title: 'Job Configuration', path: 'configuration' },
          { title: 'Cluster Configuration', path: 'cluster_configuration' }
        ]
      : JOB_MODULE_DEFAULT_CONFIG.routerTabs
  };
};

@NgModule({
  declarations: [
    ClusterConfigComponent,
    CompletedJobSubtasksTableActionComponent,
    CompletedJobTaskmanagersTableActionComponent
  ],
  imports: [
    CommonModule,
    CompletedJobRoutingModule,
    ShareModule,
    NzIconModule,
    NzSkeletonModule,
    NzAlertModule,
    NzCardModule,
    NzTableModule,
    NzEmptyModule,
    NzPipesModule,
    NzDropDownModule,
    NzModalModule,
    NzTabsModule
  ],
  providers: [
    {
      provide: JOB_OVERVIEW_MODULE_CONFIG,
      useFactory: OVERRIDE_JOB_OVERVIEW_MODULE_CONFIG_FACTORY,
      deps: [StatusService]
    },
    {
      provide: JOB_MODULE_CONFIG,
      useFactory: OVERRIDE_JOB_MODULE_CONFIG_FACTORY,
      deps: [StatusService]
    }
  ]
})
export class CompletedJobModule {}

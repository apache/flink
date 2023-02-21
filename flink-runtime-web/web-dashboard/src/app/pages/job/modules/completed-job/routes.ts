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

import { JobDetailComponent } from '@flink-runtime-web/pages/job/job-detail/job-detail.component';
import { JOB_MODULE_CONFIG, JOB_MODULE_DEFAULT_CONFIG, JobModuleConfig } from '@flink-runtime-web/pages/job/job.config';
import { ClusterConfigGuard } from '@flink-runtime-web/pages/job/modules/completed-job/cluster-config.guard';
import { CompletedJobSubtasksTableActionComponent } from '@flink-runtime-web/pages/job/modules/completed-job/subtasks-table-action/completed-job-subtasks-table-action.component';
import { CompletedJobTaskmanagersTableActionComponent } from '@flink-runtime-web/pages/job/modules/completed-job/taskmanagers-table-action/completed-job-taskmanagers-table-action.component';
import {
  JOB_OVERVIEW_MODULE_CONFIG,
  JOB_OVERVIEW_MODULE_DEFAULT_CONFIG,
  JobOverviewModuleConfig
} from '@flink-runtime-web/pages/job/overview/job-overview.config';
import { StatusService } from '@flink-runtime-web/services';

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

export const COMPLETED_JOB_ROUES: Routes = [
  {
    path: '',
    component: JobDetailComponent,
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
    ],
    children: [
      {
        path: 'overview',
        loadChildren: () => import('../../overview/routes').then(m => m.JOB_OVERVIEW_ROUTES),
        data: {
          path: 'overview'
        }
      },
      {
        path: 'timeline',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/timeline/job-timeline.component').then(m => m.JobTimelineComponent),
        data: {
          path: 'timeline'
        }
      },
      {
        path: 'exceptions',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/exceptions/job-exceptions.component').then(
            m => m.JobExceptionsComponent
          ),
        data: {
          path: 'exceptions'
        }
      },
      {
        path: 'checkpoints',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/checkpoints/job-checkpoints.component').then(
            m => m.JobCheckpointsComponent
          ),
        data: {
          path: 'checkpoints'
        }
      },
      {
        path: 'configuration',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/configuration/job-configuration.component').then(
            m => m.JobConfigurationComponent
          ),
        data: {
          path: 'configuration'
        }
      },
      {
        path: 'cluster_configuration',
        loadComponent: () =>
          import('@flink-runtime-web/pages/job/modules/completed-job/cluster-config/cluster-config.component').then(
            m => m.ClusterConfigComponent
          ),
        canActivate: [ClusterConfigGuard],
        data: {
          path: 'cluster_configuration'
        }
      },
      { path: '**', redirectTo: 'overview', pathMatch: 'full' }
    ]
  }
];

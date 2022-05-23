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

import { InjectionToken, Type } from '@angular/core';

import {
  JobOverviewSubtasksTableAction,
  SubtasksTableActionComponent
} from '@flink-runtime-web/pages/job/overview/subtasks/table-action/subtasks-table-action.component';
import {
  JobOverviewTaskManagersTableAction,
  TaskmanagersTableActionComponent
} from '@flink-runtime-web/pages/job/overview/taskmanagers/table-action/taskmanagers-table-action.component';
import { BackpressureBadgeComponent } from '@flink-runtime-web/share/customize/backpressure-badge/backpressure-badge.component';
import { JobBadgeComponent } from '@flink-runtime-web/share/customize/job-badge/job-badge.component';
import { TaskBadgeComponent } from '@flink-runtime-web/share/customize/task-badge/task-badge.component';

export interface JobOverviewModuleConfig {
  taskManagerActionComponent?: Type<JobOverviewTaskManagersTableAction>;
  subtaskActionComponent?: Type<JobOverviewSubtasksTableAction>;
  stateBadgeComponent?: Type<unknown>;
  taskCountBadgeComponent?: Type<unknown>;
  backpressureBadgeComponent?: Type<unknown>;
}

export const JOB_OVERVIEW_MODULE_DEFAULT_CONFIG: Required<JobOverviewModuleConfig> = {
  taskManagerActionComponent: TaskmanagersTableActionComponent,
  subtaskActionComponent: SubtasksTableActionComponent,
  stateBadgeComponent: JobBadgeComponent,
  taskCountBadgeComponent: TaskBadgeComponent,
  backpressureBadgeComponent: BackpressureBadgeComponent
};

export const JOB_OVERVIEW_MODULE_CONFIG = new InjectionToken<JobOverviewModuleConfig>('job-manager-module-config', {
  providedIn: 'root',
  factory: JOB_OVERVIEW_MODULE_CONFIG_FACTORY
});

function JOB_OVERVIEW_MODULE_CONFIG_FACTORY(): JobOverviewModuleConfig {
  return JOB_OVERVIEW_MODULE_DEFAULT_CONFIG;
}

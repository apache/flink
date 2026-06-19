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

import { InjectionToken } from '@angular/core';

import { BackpressureBadgeComponent } from '@flink-runtime-web/components/backpressure-badge/backpressure-badge.component';
import { DurationBadgeComponent } from '@flink-runtime-web/components/duration-badge/duration-badge.component';
import { JobBadgeComponent } from '@flink-runtime-web/components/job-badge/job-badge.component';
import { TaskBadgeComponent } from '@flink-runtime-web/components/task-badge/task-badge.component';
import { ModuleConfig } from '@flink-runtime-web/core/module-config';
import { SubtasksTableActionComponent } from '@flink-runtime-web/pages/job/overview/subtasks/table-action/subtasks-table-action.component';
import { TaskmanagersTableActionComponent } from '@flink-runtime-web/pages/job/overview/taskmanagers/table-action/taskmanagers-table-action.component';

type customComponentKeys =
  | 'taskManagerActionComponent'
  | 'subtaskActionComponent'
  | 'durationBadgeComponent'
  | 'stateBadgeComponent'
  | 'taskCountBadgeComponent'
  | 'backpressureBadgeComponent';

export type JobOverviewModuleConfig = Omit<
  ModuleConfig<string, customComponentKeys>,
  'editorOptions' | 'routerFactories'
>;

export const JOB_OVERVIEW_MODULE_DEFAULT_CONFIG: Required<JobOverviewModuleConfig> = {
  routerTabs: [
    { title: 'Detail', path: 'detail' },
    { title: 'SubTasks', path: 'subtasks' },
    { title: 'TaskManagers', path: 'taskmanagers' },
    { title: 'Watermarks', path: 'watermarks' },
    { title: 'Accumulators', path: 'accumulators' },
    { title: 'BackPressure', path: 'backpressure' },
    { title: 'Metrics', path: 'metrics' },
    { title: 'FlameGraph', path: 'flamegraph' }
  ],
  customComponents: {
    taskManagerActionComponent: TaskmanagersTableActionComponent,
    subtaskActionComponent: SubtasksTableActionComponent,
    durationBadgeComponent: DurationBadgeComponent,
    stateBadgeComponent: JobBadgeComponent,
    taskCountBadgeComponent: TaskBadgeComponent,
    backpressureBadgeComponent: BackpressureBadgeComponent
  }
};

export const JOB_OVERVIEW_MODULE_CONFIG = new InjectionToken<JobOverviewModuleConfig>('job-overview-module-config', {
  providedIn: 'root',
  factory: () => JOB_OVERVIEW_MODULE_DEFAULT_CONFIG
});

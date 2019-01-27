/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import { InjectionToken, Type } from '@angular/core';
import { VerticesNodeComponent } from './vertices-node/vertices-node.component';

export interface JobOverviewConfig {
  verticesNodeComponent?: Type<any>;
  taskManagersLogRouterGetter?: (task: {}) => string | string[];
  subTasksLogRouterGetter?: (task: {}) => string | string[];
}

export const JOB_OVERVIEW_DEFAULT_CONFIG: JobOverviewConfig = {
  verticesNodeComponent      : VerticesNodeComponent,
  taskManagersLogRouterGetter: task => [ '/task-manager', task[ 'resource-id' ], 'log' ],
  subTasksLogRouterGetter    : task => [ '/task-manager', task[ 'resource-id' ], 'log', task[ 'log-file-name' ] ]
};
export const JOB_OVERVIEW_CONFIG = new InjectionToken<JobOverviewConfig>('flink-overview-config', {
  providedIn: 'root',
  factory   : JOB_OVERVIEW_CONFIG_FACTORY
});

export function JOB_OVERVIEW_CONFIG_FACTORY(): JobOverviewConfig {
  return JOB_OVERVIEW_DEFAULT_CONFIG;
}

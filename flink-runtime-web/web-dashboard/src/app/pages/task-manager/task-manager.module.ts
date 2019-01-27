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

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NgZorroPlusModule } from '@ng-zorro/ng-plus';
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { ShareModule } from 'flink-share/share.module';
import { TaskManagerResourceComponent } from './resource/task-manager-resource.component';

import { TaskManagerRoutingModule } from './task-manager-routing.module';
import { TaskManagerListComponent } from './list/task-manager-list.component';
import { TaskManagerMetricsComponent } from './metrics/task-manager-metrics.component';
import { TaskManagerComponent } from './task-manager.component';
import { TaskManagerStatusComponent } from './status/task-manager-status.component';
import { TaskManagerLogListComponent } from './log-list/task-manager-log-list.component';
import { TaskManagerLogDetailComponent } from './log-detail/task-manager-log-detail.component';

@NgModule({
  imports     : [
    CommonModule,
    NgZorroAntdModule,
    NgZorroPlusModule,
    ShareModule,
    TaskManagerRoutingModule
  ],
  declarations: [
    TaskManagerListComponent,
    TaskManagerMetricsComponent,
    TaskManagerResourceComponent,
    TaskManagerComponent,
    TaskManagerStatusComponent,
    TaskManagerLogListComponent,
    TaskManagerLogDetailComponent
  ]
})
export class TaskManagerModule {
}

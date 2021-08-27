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

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ShareModule } from 'share/share.module';
import { TaskManagerLogDetailComponent } from './log-detail/task-manager-log-detail.component';
import { TaskManagerLogListComponent } from './log-list/task-manager-log-list.component';

import { TaskManagerRoutingModule } from './task-manager-routing.module';
import { TaskManagerListComponent } from './list/task-manager-list.component';
import { TaskManagerMetricsComponent } from './metrics/task-manager-metrics.component';
import { TaskManagerComponent } from './task-manager.component';
import { TaskManagerStatusComponent } from './status/task-manager-status.component';
import { TaskManagerThreadDumpComponent } from './thread-dump/task-manager-thread-dump.component';
import { TaskManagerLogsComponent } from './logs/task-manager-logs.component';
import { TaskManagerStdoutComponent } from './stdout/task-manager-stdout.component';
import { NzBreadCrumbModule } from 'ng-zorro-antd/breadcrumb';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

@NgModule({
  imports: [
    CommonModule,
    ShareModule,
    TaskManagerRoutingModule,
    NzTableModule,
    NzCardModule,
    NzBreadCrumbModule,
    NzIconModule,
    NzToolTipModule,
    NzProgressModule,
    NzGridModule,
    NzDividerModule,
    NzSkeletonModule
  ],
  declarations: [
    TaskManagerListComponent,
    TaskManagerMetricsComponent,
    TaskManagerComponent,
    TaskManagerStatusComponent,
    TaskManagerLogListComponent,
    TaskManagerLogDetailComponent,
    TaskManagerThreadDumpComponent,
    TaskManagerLogsComponent,
    TaskManagerStdoutComponent
  ]
})
export class TaskManagerModule {}

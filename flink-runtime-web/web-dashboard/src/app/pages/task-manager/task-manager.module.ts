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
import { FormsModule } from '@angular/forms';

import { NzBreadCrumbModule } from 'ng-zorro-antd/breadcrumb';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { ShareModule } from 'share/share.module';

import { TaskManagerListComponent } from './list/task-manager-list.component';
import { TaskManagerLogDetailComponent } from './log-detail/task-manager-log-detail.component';
import { TaskManagerLogListComponent } from './log-list/task-manager-log-list.component';
import { TaskManagerLogsComponent } from './logs/task-manager-logs.component';
import { TaskManagerMetricsComponent } from './metrics/task-manager-metrics.component';
import { TaskManagerStatusComponent } from './status/task-manager-status.component';
import { TaskManagerStdoutComponent } from './stdout/task-manager-stdout.component';
import { TaskManagerRoutingModule } from './task-manager-routing.module';
import { TaskManagerComponent } from './task-manager.component';
import { TaskManagerThreadDumpComponent } from './thread-dump/task-manager-thread-dump.component';

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
    NzSkeletonModule,
    NzCodeEditorModule,
    FormsModule
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

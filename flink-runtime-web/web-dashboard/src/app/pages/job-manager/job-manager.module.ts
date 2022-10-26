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

import { ShareModule } from '@flink-runtime-web/share/share.module';
import { NzBreadCrumbModule } from 'ng-zorro-antd/breadcrumb';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';
import { NzDescriptionsModule } from 'ng-zorro-antd/descriptions';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

import { JobManagerConfigurationComponent } from './configuration/job-manager-configuration.component';
import { JobManagerRoutingModule } from './job-manager-routing.module';
import { JobManagerComponent } from './job-manager.component';
import { JobManagerLogDetailComponent } from './log-detail/job-manager-log-detail.component';
import { JobManagerLogListComponent } from './log-list/job-manager-log-list.component';
import { JobManagerLogsComponent } from './logs/job-manager-logs.component';
import { JobManagerMetricsComponent } from './metrics/job-manager-metrics.component';
import { JobManagerStdoutComponent } from './stdout/job-manager-stdout.component';
import { JobManagerThreadDumpComponent } from './thread-dump/job-manager-thread-dump.component';

@NgModule({
  imports: [
    CommonModule,
    ShareModule,
    JobManagerRoutingModule,
    NzTableModule,
    NzProgressModule,
    NzCardModule,
    NzGridModule,
    NzIconModule,
    NzToolTipModule,
    NzBreadCrumbModule,
    NzCodeEditorModule,
    FormsModule,
    NzDescriptionsModule,
    NzSpinModule,
    NzEmptyModule
  ],
  declarations: [
    JobManagerComponent,
    JobManagerConfigurationComponent,
    JobManagerMetricsComponent,
    JobManagerLogListComponent,
    JobManagerLogDetailComponent,
    JobManagerLogsComponent,
    JobManagerStdoutComponent,
    JobManagerThreadDumpComponent
  ]
})
export class JobManagerModule {}

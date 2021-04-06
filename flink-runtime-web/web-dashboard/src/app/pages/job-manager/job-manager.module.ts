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

import { JobManagerRoutingModule } from './job-manager-routing.module';
import { JobManagerComponent } from './job-manager.component';
import { JobManagerConfigurationComponent } from './configuration/job-manager-configuration.component';
import { JobManagerLogDetailComponent } from './log-detail/job-manager-log-detail.component';
import { JobManagerLogListComponent } from './log-list/job-manager-log-list.component';
import { JobManagerLogsComponent } from './logs/job-manager-logs.component';
import { JobManagerStdoutComponent } from './stdout/job-manager-stdout.component';
import { JobManagerMetricsComponent } from './metrics/job-manager-metrics.component';

@NgModule({
  imports: [CommonModule, ShareModule, JobManagerRoutingModule],
  declarations: [
    JobManagerComponent,
    JobManagerConfigurationComponent,
    JobManagerMetricsComponent,
    JobManagerLogListComponent,
    JobManagerLogDetailComponent,
    JobManagerLogsComponent,
    JobManagerStdoutComponent
  ]
})
export class JobManagerModule {}

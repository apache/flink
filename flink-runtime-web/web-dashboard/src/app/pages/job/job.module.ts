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
import { FormsModule } from '@angular/forms';
import { NgZorroPlusModule } from '@ng-zorro/ng-plus';
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { ShareModule } from 'flink-share/share.module';

import { JobRoutingModule } from './job-routing.module';
import { JobOverviewModule } from './overview/job-overview.module';
import { JobPendingSlotsComponent } from './pending-slots/job-pending-slots.component';
import { JobStatusComponent } from './status/job-status.component';
import { JobComponent } from './job.component';
import { JobTimelineComponent } from './timeline/job-timeline.component';
import { JobConfigurationComponent } from './configuration/job-configuration.component';
import { JobExceptionsComponent } from './exceptions/job-exceptions.component';
import { JobCheckpointsComponent } from './checkpoints/job-checkpoints.component';
import { JobCheckpointsDetailComponent } from './checkpoints/detail/job-checkpoints-detail.component';
import { JobCheckpointsSubtaskComponent } from './checkpoints/subtask/job-checkpoints-subtask.component';

@NgModule({
  imports        : [
    CommonModule,
    FormsModule,
    NgZorroAntdModule,
    NgZorroPlusModule,
    ShareModule,
    JobRoutingModule,
    JobOverviewModule
  ],
  declarations   : [
    JobStatusComponent,
    JobComponent,
    JobTimelineComponent,
    JobConfigurationComponent,
    JobExceptionsComponent,
    JobCheckpointsComponent,
    JobCheckpointsDetailComponent,
    JobCheckpointsSubtaskComponent,
    JobPendingSlotsComponent
  ],
})
export class JobModule {
}

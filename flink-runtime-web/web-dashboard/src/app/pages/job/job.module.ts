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

import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzSkeletonModule } from 'ng-zorro-antd/skeleton';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { ShareModule } from 'share/share.module';

import { JobCheckpointsDetailComponent } from './checkpoints/detail/job-checkpoints-detail.component';
import { JobCheckpointsComponent } from './checkpoints/job-checkpoints.component';
import { JobCheckpointsSubtaskComponent } from './checkpoints/subtask/job-checkpoints-subtask.component';
import { JobConfigurationComponent } from './configuration/job-configuration.component';
import { JobExceptionsComponent } from './exceptions/job-exceptions.component';
import { JobRoutingModule } from './job-routing.module';
import { JobComponent } from './job.component';
import { JobStatusComponent } from './status/job-status.component';
import { JobTimelineComponent } from './timeline/job-timeline.component';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    ShareModule,
    JobRoutingModule,
    NzTableModule,
    NzButtonModule,
    NzTabsModule,
    NzCardModule,
    NzDividerModule,
    NzCollapseModule,
    NzEmptyModule,
    NzSkeletonModule,
    NzAlertModule,
    NzPopconfirmModule,
    NzIconModule,
    NzSelectModule,
    NzToolTipModule,
    NzCodeEditorModule
  ],
  declarations: [
    JobComponent,
    JobStatusComponent,
    JobExceptionsComponent,
    JobConfigurationComponent,
    JobCheckpointsComponent,
    JobCheckpointsDetailComponent,
    JobCheckpointsSubtaskComponent,
    JobTimelineComponent
  ]
})
export class JobModule {}

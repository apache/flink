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
import { RouterModule } from '@angular/router';
import { CheckpointBadgeComponent } from 'flink-share/customize/checkpoint-badge/checkpoint-badge.component';
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { NgZorroPlusModule } from '@ng-zorro/ng-plus';
import { PaginationComponent } from './common/pagination/pagination.component';
import { TaskBadgeComponent } from './customize/task-badge/task-badge.component';
import { JobBadgeComponent } from './customize/job-badge/job-badge.component';
import { JobListComponent } from './customize/job-list/job-list.component';
import { JobChartComponent } from './customize/job-chart/job-chart.component';
import { DagreModule } from 'flink-share/common/dagre/dagre.module';

@NgModule({
  imports     : [
    CommonModule,
    NgZorroAntdModule,
    NgZorroPlusModule,
    RouterModule,
    FormsModule,
    DagreModule,
  ],
  declarations: [
    TaskBadgeComponent,
    JobBadgeComponent,
    JobListComponent,
    JobChartComponent,
    CheckpointBadgeComponent,
    PaginationComponent
  ],
  exports     : [
    TaskBadgeComponent,
    JobBadgeComponent,
    JobListComponent,
    JobChartComponent,
    PaginationComponent,
    CheckpointBadgeComponent,
    DagreModule
  ]
})
export class ShareModule {
}



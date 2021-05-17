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
import { ShareModule } from 'share/share.module';
import { JobOverviewDrawerAccumulatorsComponent } from './accumulators/job-overview-drawer-accumulators.component';
import { JobOverviewDrawerBackpressureComponent } from './backpressure/job-overview-drawer-backpressure.component';
import { JobOverviewDrawerFlameGraphComponent } from './flamegraph/job-overview-drawer-flamegraph.component';
import { JobOverviewDrawerChartComponent } from './chart/job-overview-drawer-chart.component';
import { JobOverviewDrawerDetailComponent } from './detail/job-overview-drawer-detail.component';
import { JobOverviewDrawerComponent } from './drawer/job-overview-drawer.component';
import { JobOverviewRoutingModule } from './job-overview-routing.module';
import { JobOverviewComponent } from './job-overview.component';
import { JobOverviewListComponent } from './list/job-overview-list.component';
import { JobOverviewDrawerSubtasksComponent } from './subtasks/job-overview-drawer-subtasks.component';
import { JobOverviewDrawerTaskmanagersComponent } from './taskmanagers/job-overview-drawer-taskmanagers.component';
import { JobOverviewDrawerWatermarksComponent } from './watermarks/job-overview-drawer-watermarks.component';

@NgModule({
  imports: [CommonModule, FormsModule, ShareModule, JobOverviewRoutingModule],
  declarations: [
    JobOverviewComponent,
    JobOverviewDrawerComponent,
    JobOverviewListComponent,
    JobOverviewDrawerDetailComponent,
    JobOverviewDrawerTaskmanagersComponent,
    JobOverviewDrawerSubtasksComponent,
    JobOverviewDrawerChartComponent,
    JobOverviewDrawerWatermarksComponent,
    JobOverviewDrawerAccumulatorsComponent,
    JobOverviewDrawerBackpressureComponent,
    JobOverviewDrawerFlameGraphComponent
  ]
})
export class JobOverviewModule {}

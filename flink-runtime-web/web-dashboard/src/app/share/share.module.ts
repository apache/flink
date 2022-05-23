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

import { AddonModule } from '@flink-runtime-web/share/common/addon/addon.module';
import { DagreModule } from '@flink-runtime-web/share/common/dagre/dagre.module';
import { DynamicModule } from '@flink-runtime-web/share/common/dynamic/dynamic.module';
import { EditorModule } from '@flink-runtime-web/share/common/editor/editor.module';
import { FileReadDirective } from '@flink-runtime-web/share/common/file-read/file-read.directive';
import { NavigationComponent } from '@flink-runtime-web/share/common/navigation/navigation.component';
import { ResizeComponent } from '@flink-runtime-web/share/common/resize/resize.component';
import { BackpressureBadgeComponent } from '@flink-runtime-web/share/customize/backpressure-badge/backpressure-badge.component';
import { CheckpointBadgeComponent } from '@flink-runtime-web/share/customize/checkpoint-badge/checkpoint-badge.component';
import { JobBadgeComponent } from '@flink-runtime-web/share/customize/job-badge/job-badge.component';
import { JobChartComponent } from '@flink-runtime-web/share/customize/job-chart/job-chart.component';
import { JobListComponent } from '@flink-runtime-web/share/customize/job-list/job-list.component';
import { TaskBadgeComponent } from '@flink-runtime-web/share/customize/task-badge/task-badge.component';
import { PipeModule } from '@flink-runtime-web/share/pipes/pipe.module';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageModule } from 'ng-zorro-antd/message';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

import { FlameGraphComponent } from './customize/flame-graph/flame-graph.component';

@NgModule({
  imports: [
    CommonModule,
    PipeModule,
    DagreModule,
    AddonModule,
    NzCardModule,
    NzTableModule,
    NzButtonModule,
    NzDividerModule,
    NzToolTipModule,
    NzMessageModule,
    NzTabsModule,
    NzIconModule,
    EditorModule,
    DynamicModule
  ],
  declarations: [
    JobBadgeComponent,
    TaskBadgeComponent,
    JobListComponent,
    FileReadDirective,
    NavigationComponent,
    ResizeComponent,
    JobChartComponent,
    CheckpointBadgeComponent,
    BackpressureBadgeComponent,
    FlameGraphComponent
  ],
  exports: [
    JobListComponent,
    PipeModule,
    DagreModule,
    AddonModule,
    EditorModule,
    DynamicModule,
    FileReadDirective,
    NavigationComponent,
    JobBadgeComponent,
    TaskBadgeComponent,
    ResizeComponent,
    JobChartComponent,
    CheckpointBadgeComponent,
    BackpressureBadgeComponent,
    FlameGraphComponent
  ]
})
export class ShareModule {}

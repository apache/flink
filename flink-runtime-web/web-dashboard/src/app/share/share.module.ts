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
import { NgZorroAntdModule } from 'ng-zorro-antd';
import { ResizeComponent } from 'share/common/resize/resize.component';
import { CheckpointBadgeComponent } from 'share/customize/checkpoint-badge/checkpoint-badge.component';
import { JobChartComponent } from 'share/customize/job-chart/job-chart.component';
import { PipeModule } from 'share/pipes/pipe.module';
import { DagreModule } from 'share/common/dagre/dagre.module';
import { FileReadDirective } from 'share/common/file-read/file-read.directive';
import { MonacoEditorComponent } from 'share/common/monaco-editor/monaco-editor.component';
import { NavigationComponent } from 'share/common/navigation/navigation.component';
import { JobBadgeComponent } from 'share/customize/job-badge/job-badge.component';
import { JobListComponent } from 'share/customize/job-list/job-list.component';
import { TaskBadgeComponent } from 'share/customize/task-badge/task-badge.component';
import { RefreshDownloadComponent } from 'share/customize/refresh-download/refresh-download.component';
import { BackpressureBadgeComponent } from './customize/backpressure-badge/backpressure-badge.component';
import { FlameGraphComponent } from './customize/flame-graph/flame-graph.component';

@NgModule({
  imports: [CommonModule, NgZorroAntdModule, PipeModule, DagreModule],
  declarations: [
    JobBadgeComponent,
    TaskBadgeComponent,
    JobListComponent,
    FileReadDirective,
    MonacoEditorComponent,
    NavigationComponent,
    RefreshDownloadComponent,
    ResizeComponent,
    JobChartComponent,
    CheckpointBadgeComponent,
    BackpressureBadgeComponent,
    FlameGraphComponent
  ],
  exports: [
    JobListComponent,
    NgZorroAntdModule,
    PipeModule,
    DagreModule,
    FileReadDirective,
    MonacoEditorComponent,
    NavigationComponent,
    RefreshDownloadComponent,
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

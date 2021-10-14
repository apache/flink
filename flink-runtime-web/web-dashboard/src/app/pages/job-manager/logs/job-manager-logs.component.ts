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

import { ChangeDetectorRef, Component, OnInit, ViewChild, ChangeDetectionStrategy } from '@angular/core';
import { JobManagerService } from 'services';
import { MonacoEditorComponent } from 'share/common/monaco-editor/monaco-editor.component';

@Component({
  selector: 'flink-job-manager-logs',
  templateUrl: './job-manager-logs.component.html',
  styleUrls: ['./job-manager-logs.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobManagerLogsComponent implements OnInit {
  logs = '';
  @ViewChild(MonacoEditorComponent, { static: true }) monacoEditorComponent: MonacoEditorComponent;

  reload() {
    this.jobManagerService.loadLogs().subscribe(data => {
      this.monacoEditorComponent.layout();
      this.logs = data;
      this.cdr.markForCheck();
    });
  }

  constructor(private jobManagerService: JobManagerService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    this.reload();
  }
}

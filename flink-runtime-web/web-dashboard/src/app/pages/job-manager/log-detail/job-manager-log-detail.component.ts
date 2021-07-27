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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { finalize } from 'rxjs/operators';
import { JobManagerService } from 'services';
import { MonacoEditorComponent } from 'share/common/monaco-editor/monaco-editor.component';

@Component({
  selector: 'flink-job-manager-log-detail',
  templateUrl: './job-manager-log-detail.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[class.full-screen]': 'isFullScreen'
  },
  styleUrls: ['./job-manager-log-detail.component.less']
})
export class JobManagerLogDetailComponent implements OnInit {
  logs = '';
  logName = '';
  downloadUrl = '';
  isLoading = false;
  isFullScreen = false;
  @ViewChild(MonacoEditorComponent, { static: true }) monacoEditorComponent: MonacoEditorComponent;
  constructor(
    private jobManagerService: JobManagerService,
    private cdr: ChangeDetectorRef,
    private activatedRoute: ActivatedRoute
  ) {}

  reload() {
    this.isLoading = true;
    this.cdr.markForCheck();
    this.jobManagerService
      .loadLog(this.logName)
      .pipe(
        finalize(() => {
          this.isLoading = false;
          this.layoutEditor();
          this.cdr.markForCheck();
        })
      )
      .subscribe(data => {
        this.logs = data.data;
        this.downloadUrl = data.url;
      });
  }

  layoutEditor(): void {
    setTimeout(() => this.monacoEditorComponent.layout());
  }

  toggleFullScreen(fullScreen: boolean) {
    this.isFullScreen = fullScreen;
    this.layoutEditor();
  }

  ngOnInit() {
    this.logName = this.activatedRoute.snapshot.params.logName;
    this.reload();
  }
}

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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { TaskManagerDetailInterface } from 'interfaces';
import { TaskManagerService } from 'services';
import { first } from 'rxjs/operators';
import { MonacoEditorComponent } from 'share/common/monaco-editor/monaco-editor.component';

@Component({
  selector: 'flink-task-manager-log-detail',
  templateUrl: './task-manager-log-detail.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[class.full-screen]': 'isFullScreen'
  },
  styleUrls: ['./task-manager-log-detail.component.less']
})
export class TaskManagerLogDetailComponent implements OnInit {
  logs = '';
  logName = '';
  downloadUrl = '';
  isLoading = false;
  taskManagerDetail: TaskManagerDetailInterface;
  isFullScreen = false;
  @ViewChild(MonacoEditorComponent) monacoEditorComponent: MonacoEditorComponent;

  constructor(
    private taskManagerService: TaskManagerService,
    private cdr: ChangeDetectorRef,
    private activatedRoute: ActivatedRoute
  ) {}

  reloadLog() {
    this.isLoading = true;
    this.cdr.markForCheck();
    this.taskManagerService.loadLog(this.taskManagerDetail.id, this.logName).subscribe(
      data => {
        this.logs = data.data;
        this.downloadUrl = data.url;
        this.isLoading = false;
        this.layoutEditor();
        this.cdr.markForCheck();
      },
      () => {
        this.isLoading = false;
        this.layoutEditor();
        this.cdr.markForCheck();
      }
    );
  }

  toggleFullScreen(fullScreen: boolean) {
    this.isFullScreen = fullScreen;
    this.layoutEditor();
  }

  layoutEditor(): void {
    setTimeout(() => this.monacoEditorComponent.layout());
  }

  ngOnInit() {
    this.taskManagerService.taskManagerDetail$.pipe(first()).subscribe(data => {
      this.taskManagerDetail = data;
      this.logName = this.activatedRoute.snapshot.params.logName;
      this.reloadLog();
    });
  }
}

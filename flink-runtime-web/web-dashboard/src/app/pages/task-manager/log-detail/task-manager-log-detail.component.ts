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

import { ChangeDetectorRef, Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { first, flatMap } from 'rxjs/operators';
import { ConfigService, TaskManagerService } from 'flink-services';
import { NzMonacoEditorComponent } from '@ng-zorro/ng-plus';

@Component({
  selector   : 'flink-task-manager-log-detail',
  templateUrl: './task-manager-log-detail.component.html',
  styleUrls  : [ './task-manager-log-detail.component.less' ]
})
export class TaskManagerLogDetailComponent implements OnInit {
  @ViewChild(NzMonacoEditorComponent) monacoEditorComponent: NzMonacoEditorComponent;
  logs = '';
  logName = '';
  pageIndex = 1;
  pageSize = 102400;

  pageTotal = 0;

  loadLog(data, forceLayout) {
    if (forceLayout) {
      // @ts-ignore
      this.monacoEditorComponent.layout();
    }
    this.logs = data.data;
    this.pageTotal = Math.floor(data.file_size / this.pageSize) + 1;
    this.cdr.markForCheck();
  }

  pageIndexChanged(pageIndex) {
    this.pageIndex = pageIndex;
    this.taskManagerService.loadLog(this.taskManagerService.taskManagerDetail.id, this.logName, this.pageIndex, this.pageSize)
    .subscribe(data => this.loadLog(data, false));
  }

  constructor(
    public configService: ConfigService,
    private taskManagerService: TaskManagerService,
    private cdr: ChangeDetectorRef,
    private activatedRoute: ActivatedRoute) {
  }

  ngOnInit() {
    this.logName = this.activatedRoute.snapshot.params.logName;
    this.taskManagerService.taskManagerDetail$.pipe(
      first(),
      flatMap(() =>
        this.taskManagerService.loadLog(this.taskManagerService.taskManagerDetail.id, this.logName, this.pageIndex, this.pageSize)
      )
    ).subscribe(data => this.loadLog(data, true));
  }

}

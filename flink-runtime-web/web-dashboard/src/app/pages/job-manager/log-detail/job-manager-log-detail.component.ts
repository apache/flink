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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subject } from 'rxjs';
import { finalize, takeUntil } from 'rxjs/operators';

import {
  JOB_MANAGER_MODULE_CONFIG,
  JOB_MANAGER_MODULE_DEFAULT_CONFIG,
  JobManagerModuleConfig
} from '@flink-runtime-web/pages/job-manager/job-manager.config';
import { JobManagerService } from '@flink-runtime-web/services';
import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';

@Component({
  selector: 'flink-job-manager-log-detail',
  templateUrl: './job-manager-log-detail.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[class.full-screen]': 'isFullScreen'
  },
  styleUrls: ['./job-manager-log-detail.component.less']
})
export class JobManagerLogDetailComponent implements OnInit, OnDestroy {
  public logs = '';
  public logName = '';
  public downloadUrl = '';
  public isLoading = false;
  public isFullScreen = false;
  public editorOptions: EditorOptions;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobManagerService: JobManagerService,
    private readonly cdr: ChangeDetectorRef,
    private readonly activatedRoute: ActivatedRoute,
    @Inject(JOB_MANAGER_MODULE_CONFIG) readonly moduleConfig: JobManagerModuleConfig
  ) {
    this.editorOptions = moduleConfig.editorOptions || JOB_MANAGER_MODULE_DEFAULT_CONFIG.editorOptions;
  }

  public ngOnInit(): void {
    this.logName = this.activatedRoute.snapshot.params.logName;
    this.reload();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public reload(): void {
    this.isLoading = true;
    this.cdr.markForCheck();
    this.jobManagerService
      .loadLog(this.logName)
      .pipe(
        finalize(() => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.logs = data.data;
        this.downloadUrl = data.url;
      });
  }

  public toggleFullScreen(fullScreen: boolean): void {
    this.isFullScreen = fullScreen;
  }
}

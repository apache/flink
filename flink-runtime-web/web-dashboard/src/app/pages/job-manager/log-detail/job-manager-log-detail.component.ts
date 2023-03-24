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
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, RouterLinkWithHref } from '@angular/router';
import { Subject } from 'rxjs';
import { finalize, takeUntil } from 'rxjs/operators';

import { AddonInlineComponent } from '@flink-runtime-web/components/addon-inline/addon-inline.component';
import { AutoResizeDirective } from '@flink-runtime-web/components/editor/auto-resize.directive';
import {
  JOB_MANAGER_MODULE_CONFIG,
  JOB_MANAGER_MODULE_DEFAULT_CONFIG,
  JobManagerModuleConfig
} from '@flink-runtime-web/pages/job-manager/job-manager.config';
import { JobManagerService } from '@flink-runtime-web/services';
import { NzBreadCrumbModule } from 'ng-zorro-antd/breadcrumb';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';
import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';
import { NzIconModule } from 'ng-zorro-antd/icon';

@Component({
  selector: 'flink-job-manager-log-detail',
  templateUrl: './job-manager-log-detail.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[class.full-screen]': 'isFullScreen'
  },
  styleUrls: ['./job-manager-log-detail.component.less'],
  imports: [
    NzBreadCrumbModule,
    RouterLinkWithHref,
    NzIconModule,
    AddonInlineComponent,
    NzCodeEditorModule,
    FormsModule,
    AutoResizeDirective
  ],
  standalone: true
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

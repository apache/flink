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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, RouterLinkWithHref } from '@angular/router';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import { AddonInlineComponent } from '@flink-runtime-web/components/addon-inline/addon-inline.component';
import { AutoResizeDirective } from '@flink-runtime-web/components/editor/auto-resize.directive';
import { ModuleConfig } from '@flink-runtime-web/core/module-config';
import {
  TASK_MANAGER_MODULE_CONFIG,
  TASK_MANAGER_MODULE_DEFAULT_CONFIG
} from '@flink-runtime-web/pages/task-manager/task-manager.config';
import { TaskManagerService } from '@flink-runtime-web/services';
import { NzBreadCrumbModule } from 'ng-zorro-antd/breadcrumb';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';
import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';
import { NzIconModule } from 'ng-zorro-antd/icon';

@Component({
  selector: 'flink-task-manager-log-detail',
  templateUrl: './task-manager-log-detail.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    '[class.full-screen]': 'isFullScreen'
  },
  styleUrls: ['./task-manager-log-detail.component.less'],
  imports: [
    NzBreadCrumbModule,
    RouterLinkWithHref,
    NzIconModule,
    AddonInlineComponent,
    NzCodeEditorModule,
    AutoResizeDirective,
    FormsModule
  ],
  standalone: true
})
export class TaskManagerLogDetailComponent implements OnInit, OnDestroy {
  public editorOptions: EditorOptions;
  public logs = '';
  public logName = '';
  public taskManagerId: string;
  public downloadUrl = '';
  public isLoading = false;
  public isFullScreen = false;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly cdr: ChangeDetectorRef,
    private readonly activatedRoute: ActivatedRoute,
    @Inject(TASK_MANAGER_MODULE_CONFIG) readonly moduleConfig: ModuleConfig
  ) {
    this.editorOptions = moduleConfig.editorOptions || TASK_MANAGER_MODULE_DEFAULT_CONFIG.editorOptions;
  }

  public ngOnInit(): void {
    this.logName = this.activatedRoute.snapshot.params.logName;
    this.taskManagerId = this.activatedRoute.parent!.snapshot.params.taskManagerId;
    this.reloadLog();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public reloadLog(): void {
    this.isLoading = true;
    this.cdr.markForCheck();
    this.taskManagerService
      .loadLog(this.taskManagerId, this.logName)
      .pipe(takeUntil(this.destroy$))
      .subscribe(data => {
        this.logs = data.data;
        this.downloadUrl = data.url;
        this.isLoading = false;
        this.cdr.markForCheck();
      });
  }

  public toggleFullScreen(fullScreen: boolean): void {
    this.isFullScreen = fullScreen;
    this.cdr.markForCheck();
  }
}

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

import { ChangeDetectorRef, Component, OnInit, ChangeDetectionStrategy, OnDestroy, Inject } from '@angular/core';
import {catchError, takeUntil} from 'rxjs/operators';
import { ConfigService, TaskManagerService } from '@flink-runtime-web/services';
import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';
import {of, Subject} from 'rxjs';
import { ActivatedRoute } from '@angular/router';
import {
  TASK_MANAGER_MODULE_CONFIG,
  TASK_MANAGER_MODULE_DEFAULT_CONFIG,
} from '@flink-runtime-web/pages/task-manager/task-manager.config';
import {ModuleConfig} from "@flink-runtime-web/core/module-config";
import {NzCodeEditorModule} from "ng-zorro-antd/code-editor";
import {AutoResizeDirective} from "@flink-runtime-web/components/editor/auto-resize.directive";
import {FormsModule} from "@angular/forms";
import {
  AddonCompactComponent
} from "@flink-runtime-web/components/addon-compact/addon-compact.component";

@Component({
  selector: 'flink-task-manager-logs',
  templateUrl: './task-manager-logs.component.html',
  styleUrls: ['./task-manager-logs.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzCodeEditorModule, AutoResizeDirective, FormsModule, AddonCompactComponent],
  standalone: true
})
export class TaskManagerLogsComponent implements OnInit, OnDestroy {
  public editorOptions: EditorOptions;
  public logs = '';
  public loading = true;
  public taskManagerId: string;
  public downloadUrl = '';
  public downloadName = '';

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly taskManagerService: TaskManagerService,
    private readonly configService: ConfigService,
    private activatedRoute: ActivatedRoute,
    private readonly cdr: ChangeDetectorRef,
    @Inject(TASK_MANAGER_MODULE_CONFIG) readonly moduleConfig: ModuleConfig
  ) {
    this.editorOptions = moduleConfig.editorOptions || TASK_MANAGER_MODULE_DEFAULT_CONFIG.editorOptions;
  }

  public ngOnInit() {
    this.taskManagerId = this.activatedRoute.parent!.snapshot.params.taskManagerId;
    this.downloadUrl = `${this.configService.BASE_URL}/taskmanagers/${this.taskManagerId}/log`;
    this.downloadName = `taskmanager_${this.taskManagerId}_log`;
    this.reload();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public reload() {
    this.loading = true;
    this.cdr.markForCheck();
    this.taskManagerService
      .loadLogs(this.taskManagerId)
      .pipe(
        catchError(() => of('')),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.loading = false;
        this.logs = data;
        this.cdr.markForCheck();
      });
  }
}

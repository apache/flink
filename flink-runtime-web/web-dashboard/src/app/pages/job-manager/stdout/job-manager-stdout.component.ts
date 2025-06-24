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
import { FormsModule } from '@angular/forms';
import { of, Subject } from 'rxjs';
import { catchError, takeUntil } from 'rxjs/operators';

import { AddonCompactComponent } from '@flink-runtime-web/components/addon-compact/addon-compact.component';
import { AutoResizeDirective } from '@flink-runtime-web/components/editor/auto-resize.directive';
import {
  JOB_MANAGER_MODULE_CONFIG,
  JOB_MANAGER_MODULE_DEFAULT_CONFIG,
  JobManagerModuleConfig
} from '@flink-runtime-web/pages/job-manager/job-manager.config';
import { ConfigService, JobManagerService } from '@flink-runtime-web/services';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';
import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';

@Component({
  selector: 'flink-job-manager-stdout',
  templateUrl: './job-manager-stdout.component.html',
  styleUrls: ['./job-manager-stdout.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [NzCodeEditorModule, AutoResizeDirective, FormsModule, AddonCompactComponent],
  standalone: true
})
export class JobManagerStdoutComponent implements OnInit, OnDestroy {
  public readonly downloadName = `jobmanager_stdout`;
  public downloadUrl: string;
  public editorOptions: EditorOptions;
  public stdout = '';
  public loading = true;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobManagerService: JobManagerService,
    private readonly configService: ConfigService,
    private readonly cdr: ChangeDetectorRef,
    @Inject(JOB_MANAGER_MODULE_CONFIG) readonly moduleConfig: JobManagerModuleConfig
  ) {
    this.editorOptions = moduleConfig.editorOptions || JOB_MANAGER_MODULE_DEFAULT_CONFIG.editorOptions;
    this.downloadUrl = `${this.configService.BASE_URL}/jobmanager/stdout`;
  }

  public ngOnInit(): void {
    this.reload();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public reload(): void {
    this.loading = true;
    this.cdr.markForCheck();
    this.jobManagerService
      .loadStdout()
      .pipe(
        catchError(() => of('')),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        this.loading = false;
        this.stdout = data;
        this.cdr.markForCheck();
      });
  }
}

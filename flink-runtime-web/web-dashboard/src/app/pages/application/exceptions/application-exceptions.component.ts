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

import { formatDate } from '@angular/common';
import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef, OnDestroy } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Subject } from 'rxjs';
import { distinctUntilChanged, mergeMap, takeUntil, tap } from 'rxjs/operators';

import { AutoResizeDirective } from '@flink-runtime-web/components/editor/auto-resize.directive';
import { flinkEditorOptions } from '@flink-runtime-web/components/editor/editor-config';
import { ExceptionInfo } from '@flink-runtime-web/interfaces';
import { ApplicationService } from '@flink-runtime-web/services';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCodeEditorModule, EditorOptions } from 'ng-zorro-antd/code-editor';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzTooltipModule } from 'ng-zorro-antd/tooltip';

import { ApplicationLocalService } from '../application-local.service';

@Component({
  selector: 'flink-application-exceptions',
  templateUrl: './application-exceptions.component.html',
  styleUrls: ['./application-exceptions.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzTabsModule,
    NzCodeEditorModule,
    AutoResizeDirective,
    NzTableModule,
    NzSelectModule,
    NzTooltipModule,
    FormsModule,
    NzIconModule,
    NzButtonModule,
    NzTagModule
  ]
})
export class ApplicationExceptionsComponent implements OnInit, OnDestroy {
  public readonly trackByTimestamp = (_: number, node: ExceptionInfo): number => node.timestamp;

  public rootException = '';
  public isLoading = false;
  public total = 0;
  public editorOptions: EditorOptions = flinkEditorOptions;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly applicationService: ApplicationService,
    private readonly applicationLocalService: ApplicationLocalService,
    private readonly cdr: ChangeDetectorRef
  ) {}

  public ngOnInit(): void {
    this.loadMore();
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public loadMore(): void {
    this.isLoading = true;
    this.applicationLocalService
      .applicationDetailChanges()
      .pipe(
        distinctUntilChanged((pre, next) => pre.id === next.id),
        mergeMap(application => this.applicationService.loadExceptions(application.id)),
        tap(() => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(data => {
        // @ts-ignore
        const exceptionHistory = data.exceptionHistory;
        if (exceptionHistory.entries.length > 0) {
          const exceptionInfo = exceptionHistory.entries[0];
          let exceptionMessage = `${formatDate(exceptionInfo.timestamp, 'yyyy-MM-dd HH:mm:ss', 'en')}\n`;
          if (exceptionInfo.jobId) {
            exceptionMessage += `Related Job: ${exceptionInfo.jobId}\n`;
          }
          exceptionMessage += exceptionInfo.stacktrace;
          this.rootException = exceptionMessage;
        } else {
          this.rootException = 'No Root Exception';
        }
      });
  }
}

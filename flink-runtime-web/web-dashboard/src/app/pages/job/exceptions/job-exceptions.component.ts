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

import { DatePipe, formatDate, NgForOf, NgIf } from '@angular/common';
import { Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef, OnDestroy } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { Subject } from 'rxjs';
import { distinctUntilChanged, mergeMap, takeUntil, tap } from 'rxjs/operators';

import { AutoResizeDirective } from '@flink-runtime-web/components/editor/auto-resize.directive';
import { flinkEditorOptions } from '@flink-runtime-web/components/editor/editor-config';
import { ExceptionInfo, RootExceptionInfo } from '@flink-runtime-web/interfaces';
import { JobService } from '@flink-runtime-web/services';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCodeEditorModule } from 'ng-zorro-antd/code-editor';
import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

import { JobLocalService } from '../job-local.service';

interface ExceptionHistoryItem {
  /**
   * List of concurrent exceptions that caused this failure.
   */
  exceptions: ExceptionInfo[];

  /**
   * An exception from the list, that is currently selected for rendering.
   */
  selected: ExceptionInfo;

  /**
   * Should this failure be expanded in UI?
   */
  expand: boolean;
}

const stripConcurrentExceptions = function (rootException: RootExceptionInfo): ExceptionInfo {
  const { concurrentExceptions, ...mainException } = rootException;
  return mainException;
};

const markGlobalFailure = function (exception: ExceptionInfo): ExceptionInfo {
  if (exception.taskName == null) {
    exception.taskName = '(global failure)';
  }
  return exception;
};

@Component({
  selector: 'flink-job-exceptions',
  templateUrl: './job-exceptions.component.html',
  styleUrls: ['./job-exceptions.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzTabsModule,
    NzCodeEditorModule,
    AutoResizeDirective,
    NzTableModule,
    NgForOf,
    DatePipe,
    NzSelectModule,
    NzToolTipModule,
    NgIf,
    FormsModule,
    NzIconModule,
    NzButtonModule
  ],
  standalone: true
})
export class JobExceptionsComponent implements OnInit, OnDestroy {
  public readonly trackByTimestamp = (_: number, node: ExceptionInfo): number => node.timestamp;

  public rootException = '';
  public exceptionHistory: ExceptionHistoryItem[] = [];
  public truncated = false;
  public isLoading = false;
  public maxExceptions = 0;
  public total = 0;
  public editorOptions: EditorOptions = flinkEditorOptions;

  private readonly destroy$ = new Subject<void>();

  constructor(
    private readonly jobService: JobService,
    private readonly jobLocalService: JobLocalService,
    private readonly cdr: ChangeDetectorRef,
    private readonly router: Router
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
    this.maxExceptions += 10;
    this.jobLocalService
      .jobDetailChanges()
      .pipe(
        distinctUntilChanged((pre, next) => pre.jid === next.jid),
        mergeMap(job => this.jobService.loadExceptions(job.jid, this.maxExceptions)),
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
          const mostRecentException = exceptionHistory.entries[0];
          this.rootException = `${formatDate(mostRecentException.timestamp, 'yyyy-MM-dd HH:mm:ss', 'en')}\n${
            mostRecentException.stacktrace
          }`;
        } else {
          this.rootException = 'No Root Exception';
        }
        this.truncated = exceptionHistory.truncated;
        this.exceptionHistory = exceptionHistory.entries.map(entry => {
          const values = [stripConcurrentExceptions(entry)].concat(entry.concurrentExceptions).map(markGlobalFailure);
          return {
            selected: values[0],
            exceptions: values,
            expand: false
          };
        });
      });
  }

  public navigateTo(taskManagerId: string | null): void {
    if (taskManagerId !== null) {
      this.router.navigate(['task-manager', taskManagerId, 'metrics']).then();
    }
  }
}
